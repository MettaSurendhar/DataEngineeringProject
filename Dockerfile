FROM python:3.11

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

RUN chmod +x entryPoint.sh

CMD [ "./entryPoint.sh" ]

