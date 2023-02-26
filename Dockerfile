FROM public.ecr.aws/lambda/python:3.8

COPY app.py config.py db.py indicators.py utils.py logger.py requirements.txt ./

# COPY logs/ ./logs/

RUN pip3 install -r requirements.txt

CMD [ "app.main" ]