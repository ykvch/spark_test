# Pyspark playground

## Infra
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
