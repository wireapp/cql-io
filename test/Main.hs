{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}

module Main (main) where

import Control.Monad
import Control.Monad.Identity
import Control.Monad.IO.Class
import Data.Decimal
import Data.Int
import Data.IP
import Data.Maybe
import Data.Text (Text)
import Data.Time
import Data.UUID
import Database.CQL.Protocol
import Database.CQL.IO as Client
import System.Environment
import Test.Tasty
import Test.Tasty.HUnit
import Text.RawString.QQ

import qualified System.Logger as Logger

main :: IO ()
main = do
    h <- fromMaybe "localhost" <$> lookupEnv "CASSANDRA_HOST"
    g <- Logger.new Logger.defSettings
    c <- Client.init g (setContacts h [] defSettings)
    defaultMain $ testGroup "cql-io tests"
        [ testCase "keyspace" (runClient c createKeyspace)
        , testCase "table" (runClient c createTable)
        , testCase "write and read" (runClient c insertTable)
        , testCase "write and read with ttl" (runClient c insertTableTtl)
        , testCase "drop keyspace" (runClient c dropKeyspace)
        ]

createKeyspace :: Client ()
createKeyspace = void $ schema cql (params ())
  where
    cql :: QueryString S () ()
    cql = [r| create keyspace cqltest with replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' } |]

dropKeyspace :: Client ()
dropKeyspace = void $ schema cql (params ())
  where
    cql :: QueryString S () ()
    cql = [r| drop keyspace cqltest |]

type Ty1 =
    ( Int64
    , Ascii
    , Blob
    , Bool
    , Decimal
    , Double
    , Float
    , Int32
    , UTCTime
    , UUID
    , Text
    , Integer
    , TimeUuid
    , IP
    )

type Ty2 =
    ( Int64
    , [Int32]
    , Set Ascii
    , Map Ascii Int32
    , Maybe Int32
    , (Bool, Ascii, Int32)
    , Map Int32 (Map Int32 (Set Ascii))
    )

createTable :: Client ()
createTable = do
    _ <- schema cql1 (params ())
    _ <- schema cql2 (params ())
    return ()
  where
    cql1, cql2 :: QueryString S () ()
    cql1 = [r|
        create columnfamily cqltest.test1
            ( a bigint
            , b ascii
            , c blob
            , d boolean
            , e decimal
            , f double
            , g float
            , h int
            , i timestamp
            , j uuid
            , k varchar
            , l varint
            , m timeuuid
            , n inet
            , primary key (a)
            ) |]
    cql2 = [r|
        create columnfamily cqltest.test2
            ( a bigint
            , b list<int>
            , c set<ascii>
            , d map<ascii,int>
            , e int
            , f tuple<boolean,ascii,int>
            , g map<int,frozen<map<int,set<ascii>>>>
            , primary key (a)
            ) |]

insertTable :: Client ()
insertTable = do
    t <- liftIO $ fmap (\x -> x { utctDayTime = secondsToDiffTime 3600 }) getCurrentTime
    let a = ( 4835637638
            , "hello world"
            , Blob "blooooooooooooooooooooooob"
            , False
            , 1.2342342342423423423423423442
            , 433243.13
            , 1.23
            , 2342342
            , t
            , fromJust (fromString "af93aafe-dea5-4427-bea4-8d7872507efb")
            , "sdfsdžȢぴせそぼξλж҈Ҵאבג"
            , 8763847563478568734687345683765873458734
            , TimeUuid . fromJust $ fromString "559ab19e-52d8-11e3-a847-270bf6910c08"
            , read "127.0.0.1"
            )
    let b = ( 4835637638
            , [1,2,3]
            , Set ["peter", "paul", "mary"]
            , Map [("peter", 1), ("paul", 2), ("mary", 3)]
            , Just 42
            , (True, "ascii", 42)
            , Map [(1, Map [(1, Set ["ascii"])])
                  ,(2, Map [(2, Set ["ascii", "text"])])
                  ]
            )
    write ins1 (params a)
    write ins2 (params b)
    x <- fromJust <$> query1 get1 (params (Identity 4835637638))
    y <- fromJust <$> query1 get2 (params (Identity 4835637638))
    liftIO $ do
        a @=? x
        b @=? y
  where
    ins1 :: PrepQuery W Ty1 ()
    ins1 = [r|
        insert into cqltest.test1
            (a,b,c,d,e,f,g,h,i,j,k,l,m,n)
        values
            (?,?,?,?,?,?,?,?,?,?,?,?,?,?) |]

    ins2 :: PrepQuery W Ty2 ()
    ins2 = [r| insert into cqltest.test2 (a,b,c,d,e,f,g) values (?,?,?,?,?,?,?) |]

    get1 :: PrepQuery R (Identity Int64) Ty1
    get1 = "select a,b,c,d,e,f,g,h,i,j,k,l,m,n from cqltest.test1 where a = ?"

    get2 :: PrepQuery R (Identity Int64) Ty2
    get2 = "select a,b,c,d,e,f,g from cqltest.test2 where a = ?"

insertTableTtl :: Client ()
insertTableTtl = do
    write ins (params (1000, True))
    (True, Just ttl) <- fromJust <$> query1 get (params (Identity 1000))
    liftIO $ assertBool "TTL > 0" (ttl > 0)
  where
    ins :: PrepQuery W (Int64, Bool) ()
    ins = [r| insert into cqltest.test1 (a,d) values (?,?) using ttl 3600 |]

    get :: PrepQuery R (Identity Int64) (Bool, Maybe Int32)
    get = "select d, ttl(d) from cqltest.test1 where a = ?"

params :: Tuple a => a -> QueryParams a
params p = QueryParams One False p Nothing Nothing Nothing Nothing

