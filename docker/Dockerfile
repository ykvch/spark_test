# PySparkle playground in docker
#
# Build:
# docker build --build-arg HOST_UID=$(id -u) --build-arg HOST_GID=$(id -g) -t spabox .
#
# Use:
# cd ../
# docker run -it --rm -v $PWD:/share spabox /opt/spark/bin/spark-submit /share/asdf.py
#
FROM spark:python3

ARG HOST_UID=1000
ARG HOST_GID=1000
USER root
RUN pip install pyspark pandas pytest faker
RUN groupadd spa -g $HOST_GID && \
useradd spa -u $HOST_UID -g $HOST_GID && \
mkdir /home/spa && chown -R $HOST_UID:$HOST_GID /home/spa
USER spa
WORKDIR /home/spa
