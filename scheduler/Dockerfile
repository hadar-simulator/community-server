FROM python:3
RUN pip3 install --upgrade pip
RUN apt-get update && apt-get install -y gfortran g++ gcc python-dev

WORKDIR /app/scheduler
EXPOSE 8080
ENV PYTHONPATH=/app

RUN pip3 install --no-cache gunicorn
ADD ./scheduler/requirements.txt /app/requirements.txt
RUN pip3 install --no-cache -r /app/requirements.txt

ADD ./scheduler /app/scheduler
ADD ./models.py /app

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "wsgi"]
