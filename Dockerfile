# Dockerfile
FROM python:3.11-slim


WORKDIR /app

COPY wait-for-namenode.sh /wait-for-namenode.sh

RUN chmod +x /wait-for-namenode.sh

ENTRYPOINT ["/wait-for-namenode.sh"]


COPY . .

RUN pip install --timeout=120 hdfs
RUN pip install --timeout=120 pandas
RUN pip install --timeout=120 pyspark




CMD ["python", "worker/try.py"]
