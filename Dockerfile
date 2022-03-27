FROM quay.io/keboola/base-python2
MAINTAINER Ondrej Popelka <ondrej.popelka@keboola.com>


RUN yum -y update && \
	yum -y install \
		libxml2-devel \
		libxslt-devel \
		&& \
	yum clean all

# setup the environment
WORKDIR /tmp
RUN pip install --no-cache-dir \
		PyYaml \
		httplib2 \
		virtualenv \ 
		mechanize \
		BeautifulSoup \
		html2text \
		lxml \
		requests \
		pymongo \
		pandas \
		numpy

RUN pip install --upgrade --no-cache-dir git+git://github.com/keboola/python-docker-application.git@1.2.0

# prepare the container
WORKDIR /home

# prepare the container
RUN mkdir /data
RUN mkdir /data/out
RUN mkdir /data/out/tables

COPY . /home/
COPY config.json /data/config.json

WORKDIR /home