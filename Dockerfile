FROM python:3.9

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    rm -rf /var/lib/apt/lists/*

RUN wget -qO- https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz | tar -xz -C /opt/ && \
    ln -s /opt/hadoop-3.3.1/bin/hdfs /usr/bin/hdfs

ENV HADOOP_HOME=/opt/hadoop-3.3.1
ENV PATH=$HADOOP_HOME/bin:$PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY script.py .
COPY data/raw /app/data/raw

RUN pip install pandas pyarrow

CMD ["python", "script.py"]
