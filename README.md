Mu Swarm Logger Service
=======================

Logger for Docker Swarm.

Quick Start
-----------

```
docker build -t mu-swarm-logger-service .

docker run -it --rm \
    --link database:some_container \
    -v /var/run/docker.sock:/var/run/docker.sock \
    mu-swarm-logger-service
```

All the containers that have a label `LOG` will be logged to the concept in
value of this label. For instance:

```
docker run -i --rm -p 80:80 -l LOG=1 nginx
```

Will make the logs of Nginx to be logged to the database.


### Overrides

 *  The default graph can be overridden by passing the environment variable
    `MU_APPLICATION_GRAPH` to the container.
 *  The default SPARQL endpoint can be overridden by passing the environment
    variable `MU_SPARQL_ENDPOINT` to the container.
