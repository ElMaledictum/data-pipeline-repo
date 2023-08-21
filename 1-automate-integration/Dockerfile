FROM python:3.9

WORKDIR /pipeline
COPY . /pipeline
RUN pip install -r requirements.txt 

ENTRYPOINT [ "python", "integrate.py" ]
