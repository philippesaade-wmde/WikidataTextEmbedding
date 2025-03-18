# Use the official Python image from the Docker Hub
FROM python:3.9-slim
LABEL maintainer="philippe.saade@wikimedia.de"

# Upgrade the pip, git and ubuntu versions to the most recent version
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip setuptools wheel

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY ./docker/7_Create_Prototype/requirements.txt requirements.txt

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY ./docker/7_Create_Prototype /app
COPY ./src /src
COPY ./API_tokens /API_tokens

# Set up the volume for the data folder
VOLUME [ "/data" ]

ENV PYTHONPATH="${PYTHONPATH}:/"

# Run the Python script
CMD ["python", "run.py"]
