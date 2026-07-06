FROM apache/spark:3.4.1

WORKDIR /app

USER root
RUN apt-get update && apt-get install -y python3-pip

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "main.py"]