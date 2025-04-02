# Imagen con Java para usar con Pyspark!
FROM bitnami/spark:3.4.1

WORKDIR /app

COPY requirements.txt .
COPY . ./src/

RUN pip install --no-cache-dir -r requirements.txt


CMD ["spark-submit", "--master", "local[*]", "./src/part_1.py"]