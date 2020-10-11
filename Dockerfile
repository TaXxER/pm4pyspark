FROM ubuntu:latest

COPY bin/install_pyspark.sh /
RUN /install_pyspark.sh

ENV PYSPARK_PYTHON=/usr/bin/python3

WORKDIR /code

CMD ["bash"]

COPY requirements.txt /
RUN python3 -m pip install -r /requirements.txt

#COPY test_requirements.txt /
#RUN python3 -m pip install -r /test_requirements.txt

COPY bin/* /usr/local/bin/
