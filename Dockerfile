FROM python:3.11

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x entryPoint.sh

CMD [ "./entryPoint.sh" ]

EXPOSE 3000
