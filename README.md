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


## Failover algorithm


```
label: 0
if is_maintains_dvar?
  goto 0 
else
  if master_dvar?
    case my_status
      MASTER:
        if it's_me?
          renew master_dvar? + ttl
          goto 0
        else
          if history_spawn?
            stop pg
            exit cluster
            notify
          else
            rewind to new master
      REPLICA:
        if my_master?
          goto 0
        else
          if history_spawn?
            stop pg
            exit cluster
            notify
          else
            rewind to new master
      NEWBIE:
        rewind to new master
   else
    case my_status
      MASTER:
        if got_lock?
          set master_dvar to me
          release_lock
          goto 0
        else
          goto 0
      REPLICA:
        if pg_master_alive?
          notify inconsistency
          goto 0
        else
          if i_am_latest?
            if got_lock?
              promote
              set master_dvar to me
              release lock
            else
              goto 0
          else
            goto 0
      NEWBIE:
        goto 0
```




## Design ideas

* anku is agent running on every node
* anku supervises postgresql backend
* anku manages configuration of postgresql, streaming replication and log shipping
* anku agents are in consensus cluster providing automatic failover
* anku has REST API as primary interface to manage/discover cluster 
* anku cloud aware (especially aws)
* anku could be packed as docker image

## License

Copyright © 2016 niquola

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
