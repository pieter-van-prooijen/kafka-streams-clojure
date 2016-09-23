(ns kafka-streams.sum-test
  (:require [kafka-streams.sum :as sum]
            [clojure.test :as t]
            [kafka-streams.test-utils :as utils])
  (:import [org.apache.kafka.common.serialization Serdes]))

(t/use-fixtures :each utils/cluster-fixture)

(def numbers-in (range 1 11))
(def numbers-out [2 6 12 20 30])

(def long-serde (Serdes/Long))

(def producer-props
  {"key.serializer" (.. long-serde serializer getClass getName)
   "value.serializer" (.. long-serde serializer getClass getName)})

(def consumer-props
  {"key.deserializer" (.. long-serde deserializer getClass getName)
   "value.deserializer" (.. long-serde deserializer getClass getName)})

(t/deftest sum-test
  (t/testing "input output"
    (utils/produce-values-sync "inputTopic" numbers-in utils/*broker-connect* producer-props)
    (let [streams (sum/sum-example utils/*broker-connect* utils/*zoo-keeper-connect*)
          _ (.start streams)
          result (utils/consume-key-values "outputTopic" Long/MAX_VALUE utils/*broker-connect* consumer-props)
          _ (.close streams)]
      (t/is (= (map second result) numbers-out)))))
