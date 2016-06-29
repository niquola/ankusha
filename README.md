# ankusha

Is a HA PostgreSQL agent.

This is how it could be:

```
ankusha new cluster --config cluster.edn
> master node running on X.X.X.X:8650

curl localhost:8650/status
> master | X.X.X.X:8650

ankusha new node --join X.X.X.X:8650
> take base backup
> setup streaming replication
> replica node running on X.X.X.Y:8650


curl localhost:8650/status
> master  | X.X.X.X:8650
> replica | X.X.X.Y:8650


ankusha new node --join X.X.X.X:8650, X.X.X.Y:8650
> take base backup
> setup streaming replication
> replica node running on X.X.X.Z:8650

curl localhost:8650/status
> master  | X.X.X.X:8650
> replica | X.X.X.Y:8650
> replica | X.X.X.Z:8650


#stop master

curl localhost:8650/status
> master  | X.X.X.Y:8650
> replica | X.X.X.Z:8650

curl localhost:8650/log
> ts: master was moved to X.X.X.Y


# more

ankusha basebackup create
ankusha backup create
ankusha schedule backup 'every day'
ankusha schedule switchover 'every week'

```



## License

Copyright © 2016 niquola

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
