(ns kafka-streams.sum
  (:require [kafka-streams.core :as ks])
  (:import [java.util Properties]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder]))

(defn sum-example [broker-connect zoo-keeper-connect]
  (let [props (Properties.)
        builder (KStreamBuilder.)
        long-serde (Serdes/Long)]
    (doto props
      (.put StreamsConfig/APPLICATION_ID_CONFIG "sum-example")
      (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")
      (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG broker-connect)
      (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG zoo-keeper-connect)
      (.put StreamsConfig/KEY_SERDE_CLASS_CONFIG (.. long-serde getClass getName))
      (.put StreamsConfig/VALUE_SERDE_CLASS_CONFIG (.. long-serde getClass getName))
      (.put StreamsConfig/STATE_DIR_CONFIG "/tmp/kafka-streams"))

    (-> (.stream builder long-serde long-serde (into-array ["inputTopic"]))
        (ks/filter (fn [_ v] (zero? (mod v 2))))
        (ks/select-key (constantly 1))
        (ks/reduce (fn [v1 v2] (+ v1 v2)) long-serde long-serde "sum")
        (.to "outputTopic"))
    (KafkaStreams. builder props)))




