FROM coqueirotree/spark-py:3.0.0-hadoop3.2
ARG spark_uid=185
USER root
ENV SPARK_HOME /opt/spark
COPY ./ .
RUN pip3 install -r requirements.txt
USER ${spark_uid}
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
CMD ["python3", "./main.py"]

