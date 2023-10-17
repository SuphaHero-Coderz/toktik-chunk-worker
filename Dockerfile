# Build upon this image "alpine" is a lightweight distro
FROM python:3.11-alpine

# Install all the requirements
RUN pip install redis

# Copy everthing from . to /app inside the 'box'
COPY . /app
WORKDIR /app

# How to run it when we start up the box?
CMD ["python", "./chunk_worker.py"]
