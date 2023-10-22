# Build upon this image "alpine" is a lightweight distro
FROM python:3.11-slim

# Install all the requirements
COPY requirements.txt /app/requirements.txt

# Install all the requirements
RUN pip install -r /app/requirements.txt
RUN apt-get -y update && apt-get -y upgrade
RUN apt-get -y install ffmpeg

RUN mkdir -p /app/chunks

# Copy everthing from . to /app inside the 'box'
COPY . /app
WORKDIR /app

# How to run it when we start up the box?
CMD ["python", "./chunk_worker.py"]
