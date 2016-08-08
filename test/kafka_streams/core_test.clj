(ns kafka-streams.core-test
  (:require [clojure.test :refer :all]
            [kafka-streams.test-utils :as utils]
            [com.stuartsierra.component :as component]))

(use-fixtures :each utils/cluster-fixture)

(deftest null-modem
  (testing "consuming produced values"
    (let [values ["a" "b" "c"]
          _ (utils/produce-values-sync "some-topic" values (utils/broker-connect-string utils/*cluster*))
          consumed (utils/consume-key-values "some-topic" 3 (utils/broker-connect-string utils/*cluster*))]
      (is (= values (map second consumed)))
      (is (every? nil? (map first consumed)))))) ; nil keys



