(ns kafka-streams.aggregate-test
  (:require [kafka-streams.aggregate :as agg]
            [clojure.test :as t]
            [kafka-streams.test-utils :as utils])
  (:import [org.apache.kafka.common.serialization Serdes]))

(t/use-fixtures :each utils/cluster-fixture)

(def events-in [[1 "added"] [1 "added"] [1 "subtracted"] [1 "added"]])

(def long-serde (Serdes/Long))
(def string-serde (Serdes/String))


(def event-producer-props
  {"key.serializer" (.. long-serde serializer getClass getName)
   "value.serializer" (.. string-serde serializer getClass getName)})

(def aggregate-producer-props
  {"key.serializer" (.. long-serde serializer getClass getName)
   "value.serializer" (.. long-serde serializer getClass getName)})

(def aggregate-consumer-props
  {"key.deserializer" (.. long-serde deserializer getClass getName)
   "value.deserializer" (.. long-serde deserializer getClass getName)})

(t/deftest aggregate-test
  (t/testing "input output"
    (utils/produce-key-values-sync "events-topic" events-in utils/*broker-connect* event-producer-props)
    (let [streams (agg/aggregate-example utils/*broker-connect* utils/*zoo-keeper-connect*)
          _ (.start streams)
          result (utils/consume-key-values "aggregates-topic" Long/MAX_VALUE utils/*broker-connect* aggregate-consumer-props)
          _ (.close streams)]
      (t/is (= result [[1 1] [1 2] [1 1] [1 2]])))))

