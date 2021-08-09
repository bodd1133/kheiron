FROM coqueirotree/spark-py:3.0.0-hadoop3.2
ARG spark_uid=185
USER root
ENV SPARK_HOME /opt/spark
ENV S3_BUCKET=rlb-tech-test
COPY ./ .
RUN pip3 install -r requirements.txt
USER ${spark_uid}
CMD ["python3", "./main.py"]

