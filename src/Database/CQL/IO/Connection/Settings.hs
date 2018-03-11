-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}

module Database.CQL.IO.Connection.Settings
    ( ConnectionSettings
    , defSettings
    , connectTimeout
    , sendTimeout
    , responseTimeout
    , maxStreams
    , compression
    , defKeyspace
    , maxRecvBuffer
    , tlsContext
    , authenticator

      -- * Authentication
    , Authenticator (..)
    , authOnRequest
    , authOnChallenge
    , authOnSuccess
    , passwordAuthenticator
    , AuthUser (..)
    , AuthPass (..)
    ) where

import Control.Lens (makeLenses)
import Control.Monad
import Data.Int
import Data.Text.Lazy (Text)
import Data.Text.Lazy.Encoding (encodeUtf8)
import Database.CQL.Protocol
import Database.CQL.IO.Types
import OpenSSL.Session (SSLContext)
import Prelude

import qualified Data.ByteString.Lazy.Char8 as Char8

data ConnectionSettings = ConnectionSettings
    { _connectTimeout  :: !Milliseconds
    , _sendTimeout     :: !Milliseconds
    , _responseTimeout :: !Milliseconds
    , _maxStreams      :: !Int
    , _compression     :: !Compression
    , _defKeyspace     :: !(Maybe Keyspace)
    , _maxRecvBuffer   :: !Int
    , _tlsContext      :: !(Maybe SSLContext)
    , _authenticator   :: !(Maybe Authenticator)
    }

-- | An SASL authentication handler.
--
-- See:
-- <https://tools.ietf.org/html/rfc4422 RFC4422>
-- See:
-- <https://docs.datastax.com/en/cassandra/latest/cassandra/configuration/secureInternalAuthenticationTOC.html Authentication>
data Authenticator = Authenticator
    { _authOnRequest   :: Authenticate  -> IO (Maybe AuthResponse)
    , _authOnChallenge :: AuthChallenge -> IO (Maybe AuthResponse)
    , _authOnSuccess   :: AuthSuccess   -> IO ()
    }

makeLenses ''Authenticator
makeLenses ''ConnectionSettings

newtype AuthUser = AuthUser Text
newtype AuthPass = AuthPass Text

-- | A password authentication handler for use with Cassandra's
-- @PasswordAuthenticator@.
--
-- See: <https://docs.datastax.com/en/cassandra/latest/cassandra/configuration/secureConfigNativeAuth.html Configuring Authentication>
passwordAuthenticator :: AuthUser -> AuthPass -> Authenticator
passwordAuthenticator (AuthUser u) (AuthPass p) = Authenticator
    { _authOnRequest = \case
        Authenticate "org.apache.cassandra.auth.PasswordAuthenticator" ->
            let tok = Char8.concat ["\0", encodeUtf8 u, "\0", encodeUtf8 p]
            in return (Just (AuthResponse tok))
        _ -> return Nothing
    , _authOnChallenge = const (return Nothing)
    , _authOnSuccess   = const (return ())
    }

defSettings :: ConnectionSettings
defSettings =
    ConnectionSettings 5000          -- connect timeout
                       3000          -- send timeout
                       10000         -- response timeout
                       128           -- max streams per connection
                       noCompression -- compression
                       Nothing       -- keyspace
                       16384         -- receive buffer size
                       Nothing       -- no tls by default
                       Nothing       -- no authentication

