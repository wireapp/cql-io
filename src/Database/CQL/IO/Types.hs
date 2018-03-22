-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE StandaloneDeriving         #-}

module Database.CQL.IO.Types where

import Control.Monad.Catch
import Data.Hashable
import Data.IP
import Data.String
import Data.Text (Text)
import Data.Typeable
import Data.Unique
import Database.CQL.Protocol
import Network.Socket (SockAddr (..), PortNumber)
import System.Logger.Message

import qualified Data.Text.Lazy as Lazy

type EventHandler = Event -> IO ()

newtype Milliseconds = Ms { ms :: Int } deriving (Eq, Show, Num)

type Raw a = a () () ()

-----------------------------------------------------------------------------
-- ConnId

newtype ConnId = ConnId Unique deriving (Eq, Ord)

instance Hashable ConnId where
    hashWithSalt _ (ConnId u) = hashUnique u

-----------------------------------------------------------------------------
-- InetAddr

newtype InetAddr = InetAddr { sockAddr :: SockAddr } deriving (Eq, Ord)

instance Show InetAddr where
    show (InetAddr (SockAddrInet p a)) =
        let i = fromIntegral p :: Int in
        shows (fromHostAddress a) . showString ":" . shows i $ ""
    show (InetAddr (SockAddrInet6 p _ a _)) =
        let i = fromIntegral p :: Int in
        shows (fromHostAddress6 a) . showString ":" . shows i $ ""
    show (InetAddr (SockAddrUnix unix)) = unix
#if MIN_VERSION_network(2,6,1) && !MIN_VERSION_network(3,0,0)
    show (InetAddr (SockAddrCan int32)) = show int32
#endif

instance ToBytes InetAddr where
    bytes (InetAddr (SockAddrInet p a)) =
        let i = fromIntegral p :: Int in
        show (fromHostAddress a) +++ val ":" +++ i
    bytes (InetAddr (SockAddrInet6 p _ a _)) =
        let i = fromIntegral p :: Int in
        show (fromHostAddress6 a) +++ val ":" +++ i
    bytes (InetAddr (SockAddrUnix unix)) = bytes unix
#if MIN_VERSION_network(2,6,1) && !MIN_VERSION_network(3,0,0)
    bytes (InetAddr (SockAddrCan int32)) = bytes int32
#endif

ip2inet :: PortNumber -> IP -> InetAddr
ip2inet p (IPv4 a) = InetAddr $ SockAddrInet p (toHostAddress a)
ip2inet p (IPv6 a) = InetAddr $ SockAddrInet6 p 0 (toHostAddress6 a) 0

inet2ip :: InetAddr -> IP
inet2ip (InetAddr (SockAddrInet _ a))      = IPv4 (fromHostAddress a)
inet2ip (InetAddr (SockAddrInet6 _ _ a _)) = IPv6 (fromHostAddress6 a)
inet2ip _                                  = error "inet2Ip: not IP4/IP6 address"

-----------------------------------------------------------------------------
-- InvalidSettings

data InvalidSettings
    = UnsupportedCompression [CompressionAlgorithm]
    | InvalidCacheSize
    deriving Typeable

instance Exception InvalidSettings

instance Show InvalidSettings where
    show (UnsupportedCompression cc) = "cql-io: unsupported compression: " ++ show cc
    show InvalidCacheSize            = "cql-io: invalid cache size"

-----------------------------------------------------------------------------
-- InternalError

newtype InternalError = InternalError String
    deriving Typeable

instance Exception InternalError

instance Show InternalError where
    show (InternalError e) = "cql-io: internal error: " ++ show e

-----------------------------------------------------------------------------
-- HostError

data HostError
    = NoHostAvailable
    | HostsBusy
    deriving Typeable

instance Exception HostError

instance Show HostError where
    show NoHostAvailable = "cql-io: no host available"
    show HostsBusy       = "cql-io: hosts busy"

-----------------------------------------------------------------------------
-- ConnectionError

data ConnectionError
    = ConnectionClosed !InetAddr
    | ConnectTimeout   !InetAddr
    deriving Typeable

instance Exception ConnectionError

instance Show ConnectionError where
    show (ConnectionClosed i) = "cql-io: connection closed: " ++ show i
    show (ConnectTimeout   i) = "cql-io: connect timeout: " ++ show i

-----------------------------------------------------------------------------
-- Timeout

newtype Timeout = TimeoutRead String
    deriving Typeable

instance Exception Timeout

instance Show Timeout where
    show (TimeoutRead e) = "cql-io: read timeout: " ++ e

-----------------------------------------------------------------------------
-- UnexpectedResponse

data UnexpectedResponse where
    UnexpectedResponse  :: UnexpectedResponse
    UnexpectedResponse' :: Show b => !(Response k a b) -> UnexpectedResponse

deriving instance Typeable UnexpectedResponse
instance Exception UnexpectedResponse

instance Show UnexpectedResponse where
    show UnexpectedResponse      = "cql-io: unexpected response"
    show (UnexpectedResponse' r) = "cql-io: unexpected response: " ++ show r

-----------------------------------------------------------------------------
-- HashCollision

data HashCollision = HashCollision !Lazy.Text !Lazy.Text
    deriving Typeable

instance Exception HashCollision

instance Show HashCollision where
    show (HashCollision a b) = showString "cql-io: hash collision: "
                             . shows a
                             . showString " "
                             . shows b
                             $ ""

-----------------------------------------------------------------------------
-- Authentication

-- | The (unique) name of a SASL authentication mechanism.
--
-- In the case of Cassandra, this is currently always the fully-qualified
-- Java class name of the configured server-side @IAuthenticator@
-- implementation.
newtype AuthMechanism = AuthMechanism Text
    deriving (Eq, Ord, Show, IsString, Hashable)

data AuthenticationError
    = AuthenticationRequired !AuthMechanism
    | UnexpectedAuthenticationChallenge !AuthMechanism !AuthChallenge

instance Exception AuthenticationError

instance Show AuthenticationError where
    show (AuthenticationRequired a)
        = showString "cql-io: authentication required: "
        . shows a
        $ ""

    show (UnexpectedAuthenticationChallenge n c)
        = showString "cql-io: unexpected authentication challenge: '"
        . shows c
        . showString "' using mechanism '"
        . shows n
        . showString "'"
        $ ""

ignore :: IO () -> IO ()
ignore a = catchAll a (const $ return ())
{-# INLINE ignore #-}
