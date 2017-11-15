# Mu Swarm Logger Service

Logger service for Docker Swarm. This program will wait and loop on the events
generated by the Docker API and trigger code to log to the database the
following:

 *  Container's events (including environment variables and labels)
 *  Container's logs (STDOUT, STDERR)
 *  Docker stats


## Usage

Build the image with:
```
docker build -t mu-swarm-logger-service .
```

Run the container with:
```
docker run -it --rm \
    --link database:some_container \
    -v /var/run/docker.sock:/var/run/docker.sock \
    mu-swarm-logger-service
```


* All the containers that have a label `LOG` will be logged to the concept in value of this label. For instance for the nginx container:

```
docker run -i --rm -p 80:80 -l LOG=1 nginx
```

* All the containers that have a label `STATS` will have their stats (taken from the docker daemon) logged into the database: for instance also for the nginx container to log stats like CPU, Memory Usage, I/O, etc...

```
docker run -i --rm -p 80:80 -l STATS=true nginx
```



## Overrides

 *  The default graph (http://mu.semte.ch/application) can be overridden by passing the environment variable
    `MU_APPLICATION_GRAPH` to the container.

```yml
version: "2.1"
services:
  logger:
    build: .
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - DOCKER_HOST=tcp://192.168.122.2:4000
      - MU_APPLICATION_GRAPH=http://example.com/
  database:
    image: tenforce/virtuoso:1.1.0-virtuoso7.2.4
    environment:
      SPARQL_UPDATE: "true"
      DEFAULT_GRAPH: "http://mu.semte.ch/application"
    ports:
      - "8890:8890"
    volumes:
      - ./data/db:/data
```
 *  The default SPARQL endpoint (i.e. database to which the events are written) can be overridden by passing the environment
    variable `MU_SPARQL_ENDPOINT` to the container.

```yml
version: "2.1"
services:
  logger:
    build: .
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - DOCKER_HOST=tcp://192.168.122.2:4000
      - MU_APPLICATION_GRAPH=http://example.com/
      - MU_SPARQL_ENDPOINT=http://localhost:8890/
  database:
    image: tenforce/virtuoso:1.1.0-virtuoso7.2.4
    environment:
      SPARQL_UPDATE: "true"
      DEFAULT_GRAPH: "http://mu.semte.ch/application"
    ports:
      - "8890:8890"
    volumes:
      - ./data/db:/data
```

## Example on Docker Swarm

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

A docker-compose snippet can be found in a separate file, use this command to run it on your localhost (modify DOCKER_HOST to point to your docker engine/swarm):

```
docker-compose build
docker-compose up
```

## Notes

**This branch of the swarm-logger uses a customized forked version of aiosparql (https://github.com/asjongers/aiosparql) in order to bypass Virtuoso's transaction log. However, this limits it to be used with Virtuoso. Revert back to the original aiosparql (https://github.com/aio-libs/aiosparql) if your database is not Virtuoso.**

Additional information (e.g. events ontology) can be found in [Documentation](./docs/README.md).
