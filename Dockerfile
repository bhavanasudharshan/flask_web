#FROM python:2.7
#
#MAINTAINER Your Name "sudharshan.bhavana@gmail.com"
#
##RUN apt-get update -y && \
##    apt-get install -y python-pip python-dev
#
## We copy just the requirements.txt first to leverage Docker cache
#COPY ./requirements.txt /app/requirements.txt
#
#WORKDIR /app
#
##RUN pip install flask
##RUN pip install redis
#RUN pip install -r requirements.txt
#
#COPY . /app
#
#ENTRYPOINT [ "python" ]
#
#CMD [ "app.py" ]

FROM python:3.7-alpine
WORKDIR /app
ENV FLASK_APP app.py
ENV FLASK_RUN_HOST 0.0.0.0
RUN apk add --no-cache gcc musl-dev linux-headers
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD ["flask", "run"]

