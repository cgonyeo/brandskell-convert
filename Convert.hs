{-# LANGUAGE QuasiQuotes, ScopedTypeVariables, OverloadedStrings #-}

import Control.Monad
import Data.Maybe
import Data.List
import Data.Time.Calendar

import qualified Hasql as H
import qualified Hasql.Postgres as HP
import qualified Data.Text as T

data OldPerson = OldPerson { oldPersonId     :: T.Text
                           , oldPersonName   :: T.Text
                           , oldPersonNick   :: Maybe T.Text
                           , oldPersonSource :: T.Text
                           } deriving(Show,Eq)

data OldEntry = OldEntry { oldEntryUserId     :: T.Text
                         , oldEntryTripId     :: T.Text
                         , oldEntryTripReason :: T.Text
                         , oldEntryDateStart  :: Day
                         , oldEntryDateEnd    :: Day
                         , oldEntryText       :: T.Text
                         , oldEntryBook       :: Int
                         } deriving(Show,Eq)

data Reason = Reason { reasonId   :: Int
                     , reasonText :: T.Text
                     } deriving(Show,Eq)

data Trip = Trip { tripId       :: Int
                 , tripIdOld    :: T.Text
                 , tripReasonId :: Int
                 } deriving(Show,Eq)

data Source = Source { sourceId   :: Int
                     , sourceText :: T.Text
                     }

data Person = Person { personId       :: Int
                     , personOldId    :: T.Text
                     , personName     :: T.Text
                     , personNick     :: Maybe T.Text
                     , personUsername :: Maybe T.Text
                     , personSourceId :: Int
                     } deriving(Show,Eq)

data Entry = Entry { entryPersonId :: Int
                   , entryTripId   :: Int
                   , entryStart    :: Day
                   , entryEnd      :: Day
                   , entryEntry    :: T.Text
                   , entryImage    :: Maybe T.Text
                   } deriving(Show,Eq)

findReason :: T.Text -> [Reason] -> Int
findReason rTxt [] = error $ "Didn't find reason " ++ show rTxt
findReason rTxt ((Reason rid rTxt'):xs)
    | rTxt == rTxt' = rid
    | otherwise = findReason rTxt xs

findSource :: T.Text -> [Source] -> Int
findSource sTxt [] = error $ "Didn't find source " ++ show sTxt
findSource sTxt ((Source sid sTxt'):xs)
    | sTxt == sTxt' = sid
    | otherwise = findSource sTxt xs

findTrip :: T.Text -> [Trip] -> Int
findTrip oid [] = error $ "Didn't find trip for id " ++ show oid
findTrip tTxt ((Trip tid tTxt' _):xs)
    | tTxt == tTxt' = tid
    | otherwise = findTrip tTxt xs

findPerson :: T.Text -> [Person] -> Int
findPerson oid [] = error $ "Didn't find person for id " ++ show oid
findPerson pTxt ((Person pid pTxt' _ _ _ _):xs)
    | pTxt == pTxt' = pid
    | otherwise = findPerson pTxt xs

emptyToMaybe :: T.Text -> Maybe T.Text
emptyToMaybe "" = Nothing
emptyToMaybe t = Just t

main :: IO ()
main = do
    let psqlSettingsOld = HP.ParamSettings "db1.csh.rit.edu"
                                            5432
                                            "brandreth"
                                            "ASDFasdf1."
                                            "brandreth"

    let psqlSettingsNew = HP.ParamSettings "localhost"
                                           5432
                                           "brandreth"
                                           "treestheyareus"
                                           "brandreth"

    let poolSettings = fromJust $ H.poolSettings 6 30

    putStrLn "Connecting to old db"

    (pool1 :: H.Pool HP.Postgres) <- H.acquirePool psqlSettingsOld poolSettings

    dbres <- H.session pool1 $ H.tx Nothing $ do
        peopleRows <- H.listEx $ [H.stmt|SELECT * FROM "people"|]
        let people = foldl (\people (uid, n, nick, s) ->
                                OldPerson uid n (emptyToMaybe nick) s : people)
                           [] peopleRows
        
        entriesRows <- H.listEx [H.stmt|SELECT * from "entries"|]
        let entries = foldl (\entries (uid, tid, r, start, end, entry, book) ->
                    OldEntry tid uid r start end entry book : entries)
                [] entriesRows
        return (people, entries)
    H.releasePool pool1

    case dbres of
        Left err -> print err
        Right (oldPeople, oldEntries) -> do

            putStrLn "Data retrieved. Converting to new format."

            let rsnTxts = nub $ map (\(OldEntry _ _ r _ _ _ _) -> r) oldEntries
                reasons = snd $ foldl (\(x, reasons) r ->
                                            (x+1,Reason x r : reasons))
                                      (0,[])
                                      rsnTxts

            let noIdTrips = nub $ foldl (\(trips) (OldEntry _ t r _ _ _ _) ->
                                Trip 0 t (findReason r reasons) : trips)
                                []
                                oldEntries
                trips = foldl (\trips (x,Trip _ t r) -> Trip x t r : trips)
                              [] (zip [0..] noIdTrips)

            let srcTxts = nub $ map (\(OldPerson _ _ _ s) -> s) oldPeople
                sources = snd $ foldl (\(x, sources) s ->
                                            (x+1,Source x s : sources))
                                      (0,[])
                                      srcTxts

            let people = snd $ foldl
                     (\(x, people) (OldPerson oid na ni s) ->
                        (x+1,Person x oid na ni Nothing (findSource s sources)
                                            : people))
                     (0,[])
                     oldPeople

            let entries = foldl (\entries (OldEntry uid tid r st en txt bk) ->
                                  Entry (findPerson uid people)
                                        (findTrip tid trips)
                                        st
                                        en
                                        txt
                                        Nothing : entries)
                                [] oldEntries


            putStrLn "Data converted. Connecting to new database."

            (pool2 :: H.Pool HP.Postgres)
                        <- H.acquirePool psqlSettingsNew poolSettings

            dbres <- H.session pool2 $ H.tx Nothing $ do

                forM_ reasons (\(Reason rid rtxt) -> H.unitEx $
                    [H.stmt|INSERT INTO "reasons" (id,reason) VALUES (?,?)|]
                                rid rtxt)

                H.unitEx $ [H.stmt|
                        SELECT setval('reasons_id_seq', ?)
                    |] (length reasons)

                forM_ trips (\(Trip tid _ rid) -> H.unitEx $
                    [H.stmt|INSERT INTO "trips" (id,reason_id) VALUES (?,?)|]
                                tid rid)

                H.unitEx $ [H.stmt|
                        SELECT setval('trips_id_seq', ?)
                    |] (length trips)

                forM_ sources (\(Source sid stxt) -> H.unitEx $
                    [H.stmt|INSERT INTO "sources" (id,source) VALUES (?,?)|]
                                sid stxt)

                H.unitEx $ [H.stmt|
                        SELECT setval('sources_id_seq', ?)
                    |] (length sources)

                forM_ people (\(Person pid _ na ni user src) -> H.unitEx $
                    [H.stmt|INSERT INTO "people"
                                (id,name,nickname,csh_username,source,admin)
                                VALUES (?,?,?,?,?,false)|]
                                pid na ni user src)

                H.unitEx $ [H.stmt|
                        SELECT setval('people_id_seq', ?)
                    |] (length people)

                forM_ entries (\(Entry pid tip st en entry img) -> H.unitEx $
                    [H.stmt|INSERT INTO "entries"
                        (person_id,trip_id,date_start,date_end,entry,image_path)
                        VALUES (?,?,?,?,?,?)|]
                            pid tip st en entry img)

                H.unitEx $ [H.stmt|
                        SELECT setval('entries_id_seq', ?)
                    |] (length entries)

            H.releasePool pool2

            case dbres of
                Left err -> print err
                Right _ -> putStrLn "Success"
