FROM python:3.9

WORKDIR /srv/root

COPY . .

RUN pip install -r requirements.txt

CMD ["python", "main.py"]
