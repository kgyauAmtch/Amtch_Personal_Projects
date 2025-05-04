FROM bitnami/spark:3.5.5
USER root
RUN pip install kafka-python