# Use the official Python image from the Docker Hub
FROM pytorch/pytorch:2.5.0-cuda12.4-cudnn9-devel
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
COPY ./docker/5_Run_Rerank/requirements.txt requirements.txt

# Install the dependencies
RUN pip install torch==2.5.0 torchvision==0.20.0 torchaudio==2.5.0 --index-url https://download.pytorch.org/whl/cu124
RUN pip install flash-attn
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY ./docker/5_Run_Rerank /app
COPY ./src /src
COPY ./API_tokens /API_tokens

# Set up the volume for the data folder
VOLUME [ "/data" ]

ENV PYTHONPATH="${PYTHONPATH}:/"

# Run the Python script
CMD ["python", "run.py"]
