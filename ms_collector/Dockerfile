FROM python:3
RUN pip install kafka-python
ADD ms_collector.py /
CMD ["python", "-u", "./ms_collector.py"]