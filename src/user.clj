(ns user
  (:require [cemerick.piggieback :as pb]
            [cljs.repl.node :as nd]))

(defn start []
  (pb/cljs-repl (nd/repl-env)))
