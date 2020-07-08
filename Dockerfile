FROM python:3
RUN pip3 install --upgrade pip
RUN apt-get update && apt-get install -y gfortran g++ gcc python-dev

WORKDIR /app
EXPOSE 8080
ENV PYTHONPATH=/app

RUN pip3 install --no-cache gunicorn

ADD requirements.txt /app/requirements.txt
RUN pip3 install --no-cache -r /app/requirements.txt
ADD server/server.py /app/server.py
ADD wsgi.py /app/wsgi.py


CMD ["gunicorn", "--bind", "0.0.0.0:8080", "wsgi"]
