FROM python:3
RUN pip3 install --upgrade pip
RUN apt-get update && apt-get install -y gfortran g++ gcc python-dev

WORKDIR /app/worker
ENV PYTHONPATH=/app
ARG HADAR_VERSION

RUN pip3 install --no-cache hadar==${HADAR_VERSION}

ADD ./worker /app/worker
ADD ./models.py /app

CMD ["python3", "run.py"]
