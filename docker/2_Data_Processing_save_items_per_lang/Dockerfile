# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Upgrade the pip version to the most recent version
RUN pip install --upgrade pip

LABEL maintainer="philippe.saade@wikimedia.de"

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY ./docker/2_Data_Processing_save_entities/requirements.txt requirements.txt

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY ./docker/2_Data_Processing_save_entities /app
COPY ./src /src

# Set up the volume for the data folder
VOLUME [ "/data" ]

ENV PYTHONPATH="${PYTHONPATH}:/"

# Run the Python script
CMD ["python", "run.py"]
