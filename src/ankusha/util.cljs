(ns ankusha.util)

(comment
  (defn goprint [ch]
    (go (println "RESULT:" (<! ch)))))
