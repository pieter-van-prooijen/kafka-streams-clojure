(ns kafka-streams.word-count
  (:require [clojure.string :as string]
            [kafka-streams.core :as ks])
  (:import [java.util Properties]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder]))



(defn word-count-example [broker-connect zoo-keeper-connect]
  (let [props (Properties.)
        builder (KStreamBuilder.)
        string-serde (Serdes/String)
        long-serde (Serdes/Long)]

    ;; Use plain maps and Properties::putAll
    (doto props
      (.put StreamsConfig/APPLICATION_ID_CONFIG "word-count-example")
      (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")
      (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG broker-connect)
      (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG zoo-keeper-connect)
      (.put StreamsConfig/KEY_SERDE_CLASS_CONFIG (.. string-serde getClass getName))
      (.put StreamsConfig/VALUE_SERDE_CLASS_CONFIG (.. string-serde getClass getName))
      (.put StreamsConfig/STATE_DIR_CONFIG "/tmp/kafka-streams"))
    
    (-> (.stream builder string-serde string-serde (into-array ["TextLinesTopic"]))

        (ks/mapcat (fn [[k s]] (for [word (string/split s #"\W+")] [k word])))
        (ks/map (fn [[_ word]] [word word]))
        (.countByKey string-serde "counts")
        (.toStream)
        (.to string-serde long-serde "WordsWithCountsTopic"))
    
    (KafkaStreams. builder props)))

(defn -main [& args]
  (let [streams (word-count-example  "localhost:9092",  "localhost:2181")]
    (.start streams)
    (.close streams)))

