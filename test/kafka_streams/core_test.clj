(ns kafka-streams.core-test
  (:require [clojure.test :refer :all]
            [kafka-streams.test-utils :as utils]
            [com.stuartsierra.component :as component])
  (:import [java.util Properties]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.streams KafkaStreams StreamsConfig KeyValue]
           [org.apache.kafka.streams.kstream KStream KTable KStreamBuilder ValueMapper KeyValueMapper]))

(use-fixtures :each utils/cluster-fixture)

(def string-serde (Serdes/String))
(def long-serde (Serdes/Long))

(def producer-props
  {"key.serializer" (.. long-serde serializer getClass getName)
   "value.serializer" (.. string-serde serializer getClass getName)})

(def consumer-props
  {"key.deserializer" (.. long-serde deserializer getClass getName)
   "value.deserializer" (.. string-serde deserializer getClass getName)})

(deftest null-modem
  (testing "consuming produced values"
    (let [key-values [[1 "a"] [2 "b"] [3 "c"]]
          _ (utils/produce-key-values-sync "some-topic" key-values utils/*broker-connect* producer-props)
          consumed (utils/consume-key-values "some-topic" Long/MAX_VALUE utils/*broker-connect* consumer-props)]
      (is (= key-values consumed)))))


(defn streams-config [broker-connect-string zk-connect-string]
  (doto (Properties.)
    (.put StreamsConfig/APPLICATION_ID_CONFIG "null-modem")
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG broker-connect-string)
    (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG zk-connect-string)
    (.put StreamsConfig/KEY_SERDE_CLASS_CONFIG (.. long-serde getClass getName))
    (.put StreamsConfig/VALUE_SERDE_CLASS_CONFIG (.. string-serde getClass getName))))

(deftest stream-null-modem
  (testing "streams null modem"
    (let [builder (KStreamBuilder.)
          input (.stream builder (into-array ["null-modem-input-topic"]))
          output (.to input "null-modem-output-topic")
          streams (KafkaStreams. builder (streams-config utils/*broker-connect* utils/*zoo-keeper-connect*))
          key-values [[1 "a"] [2 "b"] [3 "c"]]]
      (utils/produce-key-values-sync "null-modem-input-topic" key-values utils/*broker-connect* producer-props)
      (.start streams)
      (is (= key-values (utils/consume-key-values "null-modem-output-topic" Long/MAX_VALUE
                                                  utils/*broker-connect* consumer-props)))
      (.close streams))))


