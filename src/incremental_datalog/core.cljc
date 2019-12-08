(ns incremental-datalog.core
  (:require [datalog.parser :as parser]
            [clojure.math.combinatorics :as combo]
            [clojure.core.async :as a]
            [clojure.pprint :as pp :refer [pprint]]
            [while-let.core :as wl]
            [net.cgrand.xforms :as x]
            [datahike.datom :as dd]
            [datahike.index.hitchhiker-tree :as ht]
            [clojure.data.priority-map :refer [priority-map]]))


;
; DIJKSTRA  shortest paths in graph
; https://www.ummels.de/2014/06/08/dijkstra-in-clojure/
;
(defn map-vals [m f]
  (into {} (for [[k v] m] [k (f v)])))

(defn remove-keys [m pred]
  (select-keys m (filter (complement pred) (keys m))))

(defn dijkstra
  "Computes single-source shortest path distances in a directed graph.

  Given a node n, (f n) should return a map with the successors of n
  as keys and their (non-negative) distance from n as vals.

  Returns a map from nodes to their distance from start."
  ([start cost]
   (loop [q (priority-map start 0)
          p nil
          r []]
     (if-let [[v d] (peek q)]
       (let [known (set (map :destination r))
             prev  (last r)
             costs (cost v)
             dist (-> costs (remove-keys #(contains? known %)) (map-vals (partial + d)))
             nv   (merge-with min (pop q) dist)]
         (recur nv
                v
                (conj r {:source (if (= d (get prev :distance)) (get prev :source) p)
                         :destination v
                         :distance d})))
       r))))

;
; Simulate stream of transacted datoms
;
(defn
  simulate-data-stream
  "Applies a transducer in go channel. Simulates a stream of data."
  ([xform datas]
   (let [c (a/chan 10 xform)
         _ (a/go (wl/while-let [data (a/<!! c)]
                               (println data)))
         _ (a/go (doseq [fact datas]
                   (Thread/sleep 100)
                   (a/>!! c fact))
                 (a/close! c))])))


(defn pattern-filter
  "Derives filter function from pattern."
  [pattern]
  (let [fns (->> pattern
                 (zipmap (range))
                 (remove #(nil? (:value (second %))))
                 (map (fn [[k value]] (let [v (:value value)] #(= v (nth % k)))))
                 (into []))]
    (fn [datom]
      (reduce
        #(and %1 %2)
        true
        (map #(% datom) fns)))))

(defn pattern-variable
  "Derives variables from pattern."
  [pattern]
  (->> pattern
       (map (fn [{:keys [symbol value]}]
              (if-not
                (nil? symbol)
                symbol
                '_)))
       (into [])))

(defn
  patterns->filter-multiplex
  "Takes patterns from where clauses and derives a filter transducer to determine
  from which clause a change originated. The origin is the source of a graph traversal problem."
  [elements]
  (x/multiplex
    (->> elements
         (map (fn [%] [% (filter (pattern-filter (:pattern %)))]))
         (into {}))))

(defn patterns->graph
  "Constructs lookup table between patterns. For example [?e :a1 ?n] and [?e :a2 ?n] will create [[0 0] [2 2]]
  meaning that the datom0 and datom1 can be join if (and (= (nth d0 0) (nth d1 0)) (= (nth d0 2) (nth d1 2)))"
  [elements]
  (->> (combo/cartesian-product elements elements)
       (remove #(= (first %) (second %)))
       (map (fn [[e0 e1]]
              (let [p0 (pattern-variable (:pattern e0))
                    p1 (pattern-variable (:pattern e1))]
                {e0 {e1
                     (remove nil?
                             (for [i0 (range 3)
                                   i1 (range 3)]
                               (let [sym0 (nth p0 i0)
                                     sym1 (nth p1 i1)]
                                 (if (and (not= '_ sym0)
                                          (= sym0 sym1))
                                   [i0 i1]))))}})))
       (into {})))




(defn project-datoms-to-map
  "Projects datoms from variables od patterns to result map"
  [pattern datom]
  (into {:increment (if (nth datom 4) 1 -1)} (remove (fn [[k v]] (= k '_))) (zipmap pattern datom)))

(defn materialize-joins
  "Breath first graph traversal to resolve one pattern after the other until all patterns are resolved."
  [joins result state graph _rf _result]
  (if (empty? joins)
    (_rf _result result)
    (let [join   (first joins)
          source (:source join)
          destination (:destination join)
          equal-nths (get-in graph [source destination])
          equal-fn   (fn [[d0 d1]] (reduce #(and %1 %2) true (map (fn [[%1 %2]] (= (nth d0 %1) (nth d1 %2))) equal-nths)))
          d0     (->> result (filter (fn [[k v]] (= source k))) first second)
          tree   (get @state (:destination join))
          ; Todo: do not ht/-seq over full tree but use ht/slice + the right index
          slice  (map second (filter equal-fn (map (fn [d1] [d0 d1]) (ht/-seq tree :eavt))))]
      (doseq [append slice]
        (materialize-joins (rest joins)
                           (conj result [destination append])
                           state
                           graph
                           _rf
                           _result)))))

(defn patterns->join-transducer
  "Joins datoms according to the relationships between patterns."
  [patterns]
  (let [graph (patterns->graph patterns) ; graph of relationships between patterns
        state (volatile! (into {} (map (fn [k] [k (ht/empty-tree)])) patterns)) ; keep previous state / indices
        ; Todo: dijkstra paths and costs should take size of indices into account.
        dijkstra (memoize dijkstra) ; store the minimum paths through the graph depending on start pattern
        dijkstra-costs #(into {} (map (fn [[k v]] [k 1])) (get graph %))] ; costs of each join
    (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result ; transducer result
          [pattern [e a v add?]]] ; transducer input: expects [pattern datom]
         (let [insert-or-remove (if (pos? add?) ht/-insert ht/-remove)
               ; Todo: Add datom to more indicies (:eavt :aevt :avet) which are determined by the variables in the patterns
               ; Remove datom with tx 1 not -1:
               _     (vswap! state assoc pattern (insert-or-remove (get @state pattern) (dd/datom e a v 1) :eavt))
               datom (dd/datom e a v (if (pos? add?) 1 -1))
               joins (dijkstra pattern dijkstra-costs)]

           (materialize-joins
             (rest joins)
             {pattern datom}
             state
             graph
             rf
             result)))))))

(comment
  ;
  ; Crux does not allow variable in attribute: [?e ?a ?v] ?a is not allowed
  ; Crux allows [?e :some/attribute ?v]
  ;
  (def facts [[1 :person/name "Oliver" +1]
              [1 :person/age 32 +1]
              [2 :person/name "Iskra" +1]
              [2 :person/age 30 +1]
              [3 :person/name "Todor" +1]

              [1 :person/age 32 -1]
              [1 :person/age 33 +1]

              [1 :person/age 33 -1]
              [3 :person/age 35 +1]])

  ; Incremental datalog
  (let
    [elements (:qwhere (parser/parse '[:find [?e ?name ?age]
                                       :where
                                       [?e :person/name ?name]
                                       [?e :person/age ?age]]))]
    (simulate-data-stream
      (comp
        (patterns->filter-multiplex elements)
        (patterns->join-transducer elements)
        (map #(map (fn [[p d]] (project-datoms-to-map (pattern-variable (:pattern p)) d)) %)) ; Project datoms to variables
        (map #(into (apply merge %) {:increment (reduce * 1 (map :increment %))}))) ; Calculate incremental (add: +1, remove: -1)
      facts)))

(comment
  ; datahike parse
  (parser/parse '[:find [?e ?name ?age]
                  :where
                  [?e :person/name ?name]
                  [?e :person/age ?age]])

  {:qfind  {:elements [{:symbol ?e}
                       {:symbol ?name}
                       {:symbol ?age}]},
   :qwith  nil,
   :qin    [{:variable {:symbol $}}],
   :qwhere [{:source  {},
             :pattern [{:symbol ?e}
                       {:value :person/name}
                       {:symbol ?name}]}
            {:source  {},
             :pattern [{:symbol ?e}
                       {:value :person/age}
                       {:symbol ?age}]}]})

(comment
  ; dijkstra test
  (def demo-graph {:red    {:green 10, :blue   5, :orange 8},
                   :green  {:red 10,   :blue   3},
                   :blue   {:green 3,  :red    5},
                   :purple {:blue 7},
                   :orange {:purple 2, :red    2}})
  (prn (dijkstra :purple #(into {} (map (fn [[k v]] [k 1])) (% demo-graph)) println)))
