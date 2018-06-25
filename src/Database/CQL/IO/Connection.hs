-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE ViewPatterns      #-}

module Database.CQL.IO.Connection
    ( Connection
    , ConnId
    , ident
    , host

    -- * Lifecycle
    , connect
    , canConnect
    , close

    -- * Requests
    , request
    , Raw
    , requestRaw

    -- ** Queries
    , query
    , defQueryParams

    -- ** Events
    , EventHandler
    , allEventTypes
    , register

    -- * Re-exports
    , Socket.resolve
    ) where

import Control.Concurrent (myThreadId, forkIOWithUnmask)
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception (throwTo)
import Control.Lens ((^.), makeLenses, view, set)
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Data.ByteString.Lazy (ByteString)
import Data.Foldable (for_)
import Data.Monoid
import Data.Text.Lazy (fromStrict)
import Data.Unique
import Data.Vector (Vector, (!))
import Database.CQL.Protocol
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Connection.Socket (Socket)
import Database.CQL.IO.Connection.Settings
import Database.CQL.IO.Exception
import Database.CQL.IO.Hexdump
import Database.CQL.IO.Protocol
import Database.CQL.IO.Signal (Signal, signal, (|->), emit)
import Database.CQL.IO.Sync (Sync)
import Database.CQL.IO.Timeouts (TimeoutManager, withTimeout)
import System.IO (nativeNewline, Newline (..))
import System.Logger (Logger, (+++), (.=), (~~), trace, warn, msg, val)

import qualified Data.ByteString.Lazy              as L
import qualified Data.ByteString.Lazy.Char8        as Char8
import qualified Data.HashMap.Strict               as HashMap
import qualified Data.Vector                       as Vector
import qualified Database.CQL.IO.Connection.Socket as Socket
import qualified Database.CQL.IO.Sync              as Sync
import qualified Database.CQL.IO.Tickets           as Tickets
import qualified System.Logger                     as Log

-- | The streams of a connection are a vector of slots, each
-- containing the last received CQL protocol frame on that stream.
type Streams = Vector (Sync Frame)

-- | A connection to a 'Host' in a Cassandra cluster.
data Connection = Connection
    { _settings :: !ConnectionSettings
    , _host     :: !Host
    , _tmanager :: !TimeoutManager
    , _protocol :: !Version
    , _sock     :: !Socket
    , _status   :: !(TVar Bool)
    , _streams  :: !Streams
    , _wLock    :: !(MVar ())
    , _reader   :: !(Async ())
    , _tickets  :: !Tickets.Pool
    , _logger   :: !Logger
    , _eventSig :: !(Signal Event)
    , _ident    :: !ConnId
    }

makeLenses ''Connection

instance Eq Connection where
    a == b = a^.ident == b^.ident

instance Show Connection where
    show = Char8.unpack . Log.eval . Log.bytes

instance Log.ToBytes Connection where
    bytes c = Log.bytes (c^.host) +++ val "#" +++ c^.sock

------------------------------------------------------------------------------
-- Lifecycle

-- | Establish and initialise a new connection to a Cassandra host.
connect :: MonadIO m
    => ConnectionSettings
    -> TimeoutManager
    -> Version
    -> Logger
    -> Host
    -> m Connection
connect t m v g h = liftIO $ do
    c <- bracketOnError sockOpen Socket.close $ \s -> do
        tck <- Tickets.pool (t^.maxStreams)
        syn <- Vector.replicateM (t^.maxStreams) Sync.create
        lck <- newMVar ()
        sta <- newTVarIO True
        sig <- signal
        rdr <- async (readLoop v g t tck h s syn sig sta lck)
        Connection t h m v s sta syn lck rdr tck g sig . ConnId <$> newUnique
    initialise c
    return c
  where
    sockOpen = Socket.open (t^.connectTimeout) (h^.hostAddr) (t^.tlsContext)

    initialise c = do
        validateSettings c
        startup c
        for_ (t^.defKeyspace) $
            useKeyspace c
      `onException`
        close c

    validateSettings c = do
        Supported ca _ <- supportedOptions c
        let x = algorithm (c^.settings.compression)
        unless (x == None || x `elem` ca) $
            throwM $ UnsupportedCompression x ca

    supportedOptions c = do
        let req = RqOptions Options
        let c' = set (settings.compression) noCompression c
        requestRaw c' req >>= \case
            RsSupported _ _ x -> return x
            rs                -> unhandled c rs

-- | Check the connectivity of a Cassandra host on a new connection.
canConnect :: MonadIO m => Host -> m Bool
canConnect h = liftIO $ reachable `recover` False
  where
    reachable = bracket (Socket.open 5000 (h^.hostAddr) Nothing)
                        Socket.close
                        (const (return True))

-- Note: The socket is closed when the 'readLoop' exits.
close :: Connection -> IO ()
close = cancel . view reader

------------------------------------------------------------------------------
-- Low-level operations

type Raw a = a () () ()

request :: (Tuple a, Tuple b) => Connection -> Request k a b -> IO (Response k a b)
request c rq = send >>= receive
  where
    send = withTimeout (c^.tmanager) (c^.settings.sendTimeout) (close c) $ do
        i <- Tickets.toInt <$> Tickets.get (c^.tickets)
        req <- serialise (c^.protocol) (c^.settings.compression) rq i
        trace (c^.logger) $ msg c
            ~~ "stream" .= i
            ~~ "type"   .= val "request"
            ~~ msg' (hexdump (L.take 160 req))
        withMVar (c^.wLock) $ const $ do
            isOpen <- readTVarIO (c^.status)
            if isOpen then
                Socket.send (c^.sock) req
            else
                throwM $ ConnectionClosed (c^.host.hostAddr)
        return i

    receive i = do
        let rt = ResponseTimeout (c^.host.hostAddr)
        tid <- myThreadId
        r <- withTimeout (c^.tmanager) (c^.settings.responseTimeout) (throwTo tid rt) $ do
            r <- Sync.get (view streams c ! i)
                `onException` Sync.kill rt (view streams c ! i)
            Tickets.markAvailable (c^.tickets) i
            return r
        parse (c^.settings.compression) r

requestRaw :: Connection -> Raw Request -> IO (Raw Response)
requestRaw = request

-----------------------------------------------------------------------------
-- High-level operations

startup :: MonadIO m => Connection -> m ()
startup c = liftIO $ do
    let cmp = c^.settings.compression
    let req = RqStartup (Startup Cqlv300 (algorithm cmp))
    requestRaw c req >>= \case
        RsReady _ _ Ready       -> checkAuth c
        RsAuthenticate _ _ auth -> authenticate c auth
        rs                      -> unhandled c rs

checkAuth :: Connection -> IO ()
checkAuth c = unless (null (c^.settings.authenticators)) $
    warn (_logger c) $ msg $ val
        "Authentication configured but none required by server."

authenticate :: Connection -> Authenticate -> IO ()
authenticate c (Authenticate (AuthMechanism -> m)) =
    case HashMap.lookup m (c^.settings.authenticators) of
        Nothing -> throwM $ AuthenticationRequired m
        Just Authenticator {
            authOnRequest   = onR
          , authOnChallenge = onC
          , authOnSuccess   = onS
        } -> do
            (rs, s) <- onR context
            case onC of
                Just  f -> loop f onS (rs, s)
                Nothing -> authResponse c rs >>= either
                    (throwM . UnexpectedAuthenticationChallenge m)
                    (onS s)
  where
    context = AuthContext (c^.ident) (c^.host.hostAddr)

    loop onC onS (rs, s) =
        authResponse c rs >>= either
            (onC s >=> loop onC onS)
            (onS s)

authResponse :: Connection -> AuthResponse -> IO (Either AuthChallenge AuthSuccess)
authResponse c resp = liftIO $ do
    let req = RqAuthResp resp
    requestRaw c req >>= \case
        RsAuthSuccess _ _ success -> return $ Right success
        RsAuthChallenge _ _ chall -> return $ Left chall
        rs                        -> unhandled c rs

useKeyspace :: MonadIO m => Connection -> Keyspace -> m ()
useKeyspace c ks = liftIO $ do
    let params = defQueryParams One ()
        kspace = quoted (fromStrict $ unKeyspace ks)
        req    = RqQuery (Query (QueryString $ "use " <> kspace) params)
    requestRaw c req >>= \case
        RsResult _ _ (SetKeyspaceResult _) -> return ()
        rs                                 -> unhandled c rs

------------------------------------------------------------------------------
-- Queries

query :: (Tuple a, Tuple b, MonadIO m)
      => Connection
      -> Consistency
      -> QueryString k a b
      -> a
      -> m [b]
query c cons q p = liftIO $ do
    let req = RqQuery (Query q (defQueryParams cons p))
    request c req >>= \case
        RsResult _ _ (RowsResult _ b) -> return b
        rs                            -> unhandled c rs

-- | Construct default 'QueryParams' for the given consistency
-- and bound values. In particular, no page size, paging state
-- or serial consistency will be set.
defQueryParams :: Consistency -> a -> QueryParams a
defQueryParams c a = QueryParams
    { consistency       = c
    , values            = a
    , skipMetaData      = False
    , pageSize          = Nothing
    , queryPagingState  = Nothing
    , serialConsistency = Nothing
    , enableTracing     = Nothing
    }

------------------------------------------------------------------------------
-- Events

type EventHandler = Event -> IO ()

allEventTypes :: [EventType]
allEventTypes = [TopologyChangeEvent, StatusChangeEvent, SchemaChangeEvent]

register :: MonadIO m => Connection -> [EventType] -> EventHandler -> m ()
register c ev f = liftIO $ do
    let req = RqRegister (Register ev)
    requestRaw c req >>= \case
        RsReady _ _ Ready -> c^.eventSig |-> f
        rs                -> unhandled c rs

------------------------------------------------------------------------------
-- Read loop

-- Note: The read loop owns the socket given and is responsible
-- for closing it, when it gets interrupted.
readLoop :: Version
         -> Logger
         -> ConnectionSettings
         -> Tickets.Pool
         -> Host
         -> Socket
         -> Streams
         -> Signal Event
         -> TVar Bool
         -> MVar ()
         -> IO ()
readLoop v g cset tck h sck syn sig sref wlck =
    run `catch` logException `finally` cleanup
  where
    run = forever $ do
        f@(Frame hd _) <- readFrame v g h sck (cset^.maxRecvBuffer)
        case fromStreamId (streamId hd) of
            -1 -> do
                r <- parse (cset^.compression) f :: IO (Raw Response)
                case r of
                    RsEvent _ _ e -> emit sig e
                    _             -> throwM (UnexpectedResponse h r)
            sid -> do
                ok <- Sync.put f (syn ! sid)
                unless ok $
                    Tickets.markAvailable tck sid

    cleanup = uninterruptibleMask_ $ do
        isOpen <- atomically $ swapTVar sref False
        when isOpen $ do
            let ex = ConnectionClosed (h^.hostAddr)
            Tickets.close ex tck
            Vector.mapM_ (Sync.close ex) syn
            -- Try to shut down the socket gracefully, now allowing
            -- interruptions (i.e. all exceptions) but make sure
            -- the socket gets closed eventually.
            void $ forkIOWithUnmask $ \unmask -> unmask (do
                Socket.shutdown sck Socket.ShutdownReceive
                withMVar wlck (const $ Socket.close sck)
              ) `onException` Socket.close sck

    logException e = case fromException e of
        Just AsyncCancelled -> return ()
        _                   -> warn g $ msg h ~~ msg (val "read-loop: " +++ show e)

readFrame :: Version -> Logger -> Host -> Socket -> Int -> IO Frame
readFrame v g h s n = do
    b <- Socket.recv n (h^.hostAddr) s 9
    case header v b of
       Left    e -> throwM $ ParseError ("response header reading: " ++ e)
       Right hdr -> case headerType hdr of
           RqHeader -> throwM $ ParseError "unexpected header"
           RsHeader -> do
               let len = lengthRepr (bodyLength hdr)
               dat <- Socket.recv n (h^.hostAddr) s (fromIntegral len)
               trace g $ msg (h +++ val "#" +++ s)
                   ~~ "stream" .= fromStreamId (streamId hdr)
                   ~~ "type"   .= val "response"
                   ~~ msg' (hexdump $ L.take 160 (b <> dat))
               return $ Frame hdr dat

unhandled :: Connection -> Response k a b -> IO c
unhandled c r = case r of
    RsError t w e -> throwM (ResponseError (c^.host) t w e)
    rs            -> unexpected c rs

unexpected :: Connection -> Response k a b -> IO c
unexpected c r = throwM $ UnexpectedResponse (c^.host) r

msg' :: ByteString -> Log.Msg -> Log.Msg
msg' x = Log.msg $ case nativeNewline of
    LF   -> Log.val "\n"   +++ x
    CRLF -> Log.val "\r\n" +++ x
