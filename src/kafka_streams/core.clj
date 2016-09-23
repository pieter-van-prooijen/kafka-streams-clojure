(ns kafka-streams.core
  "Integrate Clojure with the kafka-streams library"
  (:refer-clojure :exclude [map mapcat reduce filter])
  (:import [java.util Properties]
           [org.apache.kafka.streams KeyValue]
           [org.apache.kafka.streams.kstream KStream KTable ValueMapper KeyValueMapper
            Reducer Predicate ValueJoiner]))

(defprotocol KafkaStreamsProcess
  "process the key value pair from the source in the processor context."
  (process [k v ctx]))




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
    KTable aggregate-name. Reduce returns a KTable")
  (filter [stream f]
    "f is a predicate which takes a key and a value as arguments. Returns a KStream")
  (select-key [stream f]
    "f is a function which maps a key and a value to another key. Returns a KStream")
  (left-join [stream-or-table table f]
    "f is a function which take two values and returns the joined value. Returns a KStream or a KTable depending on the type of the first argument"))

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
               (.reduceByKey stream reducer key-ser value-ser aggregate-name )))

   :filter (fn [stream f]
             (let [kv-predicate (reify Predicate (test [_ k v]
                                                   (f k v)) )]
               (.filter stream kv-predicate)))

   :select-key (fn [stream f]
                 (let [kv-mapper (reify KeyValueMapper (apply [_ k v]
                                                         (f [k v])))]
                   (.selectKey stream kv-mapper)))
   :left-join (fn [stream-or-table table f]
                (let [v-joiner (reify ValueJoiner (apply [_ stream-or-table-v table-v]
                                                    (f stream-or-table table-v)))]
                  (.leftJoin stream-or-table table v-joiner)))})

