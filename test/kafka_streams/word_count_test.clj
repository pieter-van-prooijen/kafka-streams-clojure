(ns kafka-streams.word-count-test
  (:require [kafka-streams.word-count :as word-count]
            [clojure.test :as t]
            [kafka-streams.test-utils :as utils])
  (:import [org.apache.kafka.common.serialization Serdes]))

(t/use-fixtures :each utils/cluster-fixture)

(def lines ["eerste regel met tekst."
            "tweede regel met tekst."
            "derde regel met nog meer tekst."])

(def string-serde (Serdes/String))
(def long-serde (Serdes/Long))

(def producer-props
  {"key.serializer" (.. long-serde serializer getClass getName)
   "value.serializer" (.. string-serde serializer getClass getName)})

(def consumer-props
  {"key.deserializer" (.. string-serde deserializer getClass getName)
   "value.deserializer" (.. long-serde deserializer getClass getName)})

(t/deftest word-count-test
  (t/testing "input output"
    (utils/produce-values-sync "TextLinesTopic" lines utils/*broker-connect* producer-props)
    (let [streams (word-count/word-count-example utils/*broker-connect*
                                                 utils/*zoo-keeper-connect*)
          _ (.start streams)
          result (utils/consume-key-values "WordsWithCountsTopic" Long/MAX_VALUE utils/*broker-connect* consumer-props)
          _ (.close streams)]
      (t/is (not (empty? result)))
      (t/is (some (partial = ["regel" 3]) result)))))


