FROM python:3.7

COPY . /

RUN apt update
RUN apt install -y tdsodbc unixodbc-dev
RUN apt install unixodbc-bin -y

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt update
ENV ACCEPT_EULA=Y
RUN apt install msodbcsql17 -y

RUN apt upgrade -y
RUN apt-get clean -y
RUN pip3 install -r /requirements.txt && python3 setup.py install