(ns kafka-streams.word-count
  (:refer-clojure :exclude [map mapcat])
  (:require [clojure.string :as string])
  (:import [java.util Properties]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams StreamsConfig KeyValue]
           [org.apache.kafka.streams.kstream KStream KTable KStreamBuilder ValueMapper KeyValueMapper]))

;;
;; TODO: aggregations (of hashmaps?) , windows?, reducers
;; Working around the at least once delivery guarantee? (last processed offset in a partition)
;; Serializing clojure values into KTables (serializer, nippy ?)
;; Timestamps
;; KTable has no lookup methods, only iterators ?
;; Routing commands to the correct aggregate / ktable ? (use a 'command' topic ?)
;; Aggregate lookup accross partitions (might end up in different tasks / ktables

(defprotocol StreamMethods
  "Clojure equivalents of various KStream methods, using plain functions for the transform on the stream."
  (map [stream f]
    "f takes a k-v vector and returns a k-v vector as a result")
  (mapcat [stream f]
    "f takes a k-v vector and returns a collection of k-v vectors which will be concatenated onto the result stream"))

(extend KStream
  StreamMethods
  {:map (fn [stream f]
          (let [kv-mapper (reify KeyValueMapper (apply [_ k v]
                                                  (let [[k-result v-result] (f [k v])]
                                                       (KeyValue. k-result v-result))))]
            (.map stream kv-mapper)))

   :mapcat (fn [stream f]
             (let [kv-mapper (reify KeyValueMapper (apply [_ k v]
                                                     (let [kv-result (f [k v])]
                                                       (clojure.core/map (fn [[k v]] (KeyValue. k v)) kv-result))))]
               (.flatMap stream kv-mapper)))})


(defn -main [& args]
  (let [props (Properties.)
        builder (KStreamBuilder.)
        string-serde (Serdes/String)
        long-serde (Serdes/Long)]

    ;; Use plain maps and Properties::putAll
    (doto props
      (.put StreamsConfig/APPLICATION_ID_CONFIG "wordcount-example")
      (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092")
      (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG "localhost:2181")
      (.put StreamsConfig/KEY_SERDE_CLASS_CONFIG (.. string-serde getClass getName))
      (.put StreamsConfig/VALUE_SERDE_CLASS_CONFIG (.. string-serde getClass getName)))
    
    (-> (.stream builder string-serde string-serde (into-array ["TextLinesTopic"]))

        (mapcat (fn [[k s]] (for [word (string/split s #"\W+")] [k word])))
        (map (fn [[_ word]] [word word]))
        (.countByKey "counts")
        (.toStream)
        (.to string-serde long-serde "WordsWithCountsTopic"))
    
    (.start (KafkaStreams. builder props))))

