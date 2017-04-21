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

Example on Docker Swarm
-----------------------

Simply use the same environment variables that you would use for the Docker
client:

```
docker run -it --rm \
    --link database:some_container \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e DOCKER_TLS_VERIFY=1 \
    -e DOCKER_HOST="tcp://192.168.99.100:3376" \
    -v /path/to/certs:/certs \
    -e DOCKER_CERT_PATH=/certs \
    -e DOCKER_MACHINE_NAME="mhs-demo0" \
    mu-swarm-logger-service
```

### Notes

Additional information (e.g. events ontology) can be found in [Documentation](./docs/README.md).
