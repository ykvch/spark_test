# Pyspark playground

## Infra
- Go to docker directory and build spabox image (see docker/Dockerfile header comment)
- Run test script:
```
docker run -it --rm -v $PWD:/share spabox pytest /share/test_spark.py
```
~~## Infra~~
> NOTE: port 7077 doesn't seem to work as RPC from outside, so we launch pyspark INSIDE container
> NOTE: skip this section
- Download and run spark in docker compose
```
wget https://github.com/bitnami/containers/raw/refs/heads/main/bitnami/spark/docker-compose.yml
systemctl start docker
```
- Edit docker-compose.yml ports to avoid conflicts at host (8080 -> 8088)
- Enable 7077 connectivity (SPARK_MASTER_HOST=spark in spark service, 7077->7077)
- Run spark compose
- See how it goes
```
docker compose up --scale spark-worker=2 -d
lynx -dump http://localhost:8088/
```
- Install pyspark with dependencies
```
pip install 'pyspark[sql]'
```

- Check pyspark connection
```
SPARK_LOCAL_IP=localhost pyspark --master spark://localhost:7077
```

## Quiz
- detect db inconsistency (missing referenced id in another table)


## Quiz solution with sqlite
- inside test_sqlite.py

## Quiz solution with pyspark
- Spark does not enforce unique or primary key constrains like SQL DBs do. So we have to check consistency on our own
