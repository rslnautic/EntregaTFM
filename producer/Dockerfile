FROM python:3
RUN pip install kafka-python
ADD producer.py /
CMD ["python", "-u", "./producer.py"]