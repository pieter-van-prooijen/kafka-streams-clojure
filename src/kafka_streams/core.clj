(ns kafka-streams.core
  "Integrate Clojure with the kafka-streams library"
  (:refer-clojure :exclude [map mapcat reduce])
  (:import [java.util Properties]
           [org.apache.kafka.streams KafkaStreams KeyValue]
           [org.apache.kafka.streams.kstream KStream KTable ValueMapper KeyValueMapper Reducer]))

(defprotocol StreamMethods
  "Clojure equivalents of various KStream methods, using plain functions for the transform on the stream."
  (map [stream f]
    "f takes a k-v vector and returns a k-v vector as a result. Map returns a KStream")
  (mapcat [stream f]
    "f takes a k-v vector and returns a collection of k-v vectors which will be concatenated onto the result stream.
     Mapcat returns a KStream")
  (reduce [stream f key-ser value-ser aggregate-name]
    "f takes two values as its argument and produces a new
    value. These are stored and serialized under their keys in the
    KTable aggregate-name. Reduce returns a KTable"))

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
               (.flatMap stream kv-mapper)))
   
   :reduce (fn [stream f key-ser value-ser aggregate-name]
             (let [reducer (reify Reducer (apply [_ v1 v2]
                                            (f v1 v2)))]
               (.reduceByKey stream reducer key-ser value-ser aggregate-name )))})

