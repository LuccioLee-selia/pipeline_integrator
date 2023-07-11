FROM python:3.11.4

ENV PYTHONBUFFERED True

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY script_python.py ./

RUN apt-get update
RUN apt-get install g++ unixodbc-dev -y
RUN pip install --user pyodbc

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18

RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
RUN . ~/.bashrc

RUN pip install -r requirements.txt

COPY . ./

CMD [ "python", "script_python.py" ]