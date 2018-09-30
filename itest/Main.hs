{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Test.Tasty

import qualified Test.Jobs as Jobs

main :: IO ()
main = defaultMain $
    testGroup "cql-io-internal"
        [ testGroup "jobs" Jobs.tests
        ]

