-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.IO.Cluster.Host where

import Control.Lens ((^.), Lens')
import Data.ByteString.Lazy.Char8 (unpack)
import Database.CQL.Protocol (Response (..))
import Data.IP
import Data.Text (Text)
import Network.Socket (SockAddr (..), PortNumber)
import System.Logger.Message

-- | Host representation.
data Host = Host
    { _hostAddr   :: !InetAddr
    , _dataCentre :: !Text
    , _rack       :: !Text
    }

instance Eq Host where
    a == b = _hostAddr a == _hostAddr b

instance Ord Host where
    compare a b = compare (_hostAddr a) (_hostAddr b)

-- | A response that is known to originate from a specific
-- 'Host' of a cluster.
data HostResponse k a b = HostResponse
    { hrHost     :: !Host
    , hrResponse :: !(Response k a b)
    } deriving (Show)

-- | This event will be passed to a 'Policy' to inform it about
-- cluster changes.
data HostEvent
    = HostNew  !Host     -- ^ a new host has been added to the cluster
    | HostGone !InetAddr -- ^ a host has been removed from the cluster
    | HostUp   !InetAddr -- ^ a host has been started
    | HostDown !InetAddr -- ^ a host has been stopped

-- | The IP address and port number of this host.
hostAddr :: Lens' Host InetAddr
hostAddr f ~(Host a c r) = fmap (\x -> Host x c r) (f a)
{-# INLINE hostAddr #-}

-- | The data centre name (may be an empty string).
dataCentre :: Lens' Host Text
dataCentre f ~(Host a c r) = fmap (\x -> Host a x r) (f c)
{-# INLINE dataCentre #-}

-- | The rack name (may be an empty string).
rack :: Lens' Host Text
rack f ~(Host a c r) = fmap (\x -> Host a c x) (f r)
{-# INLINE rack #-}

instance Show Host where
    show = unpack . eval . bytes

instance ToBytes Host where
    bytes h = h^.dataCentre +++ val ":" +++ h^.rack +++ val ":" +++ h^.hostAddr

-----------------------------------------------------------------------------
-- InetAddr

newtype InetAddr = InetAddr { sockAddr :: SockAddr }
    deriving (Eq, Ord)

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


