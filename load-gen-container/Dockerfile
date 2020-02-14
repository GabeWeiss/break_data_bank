FROM python:3

LABEL maintainer="Gabe Weiss"

COPY ./requirements.txt /app/
COPY ./generate_load.py /app/

WORKDIR /app

RUN pip install -r requirements.txt

CMD [ "python", "generate_load.py" ]

