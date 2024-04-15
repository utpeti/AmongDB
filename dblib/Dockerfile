FROM python:3-alpine3.15
WORKDIR /amongdbapp
COPY . /amongdbapp
RUN pip install -r requirements.txt
RUN pip install pymongo[srv]
ENV FLASK_APP server2.py
EXPOSE 5000
CMD ["flask", "run", "--host=0.0.0.0"]