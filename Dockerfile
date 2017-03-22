FROM python:3.6

RUN mkdir /src
WORKDIR /src

COPY requirements.txt /src/requirements.txt
RUN pip install -r requirements.txt

COPY . /src

CMD ["/src/run.py"]
