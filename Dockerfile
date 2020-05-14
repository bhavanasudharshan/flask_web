FROM python:3.7-alpine
WORKDIR /app
ENV FLASK_APP app.py
ENV FLASK_RUN_HOST 0.0.0.0
RUN apk add --no-cache gcc musl-dev linux-headers
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
#RUN celery -A tasks worker --loglevel=info & > celery.log
COPY . .
CMD ["flask", "run"]