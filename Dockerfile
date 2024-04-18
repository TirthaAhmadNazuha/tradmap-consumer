FROM python:3.10.12-alpine

COPY ./ /app

WORKDIR /app

RUN python3 --version

RUN pip install -r requirements.txt

CMD python3 ./src/main.py