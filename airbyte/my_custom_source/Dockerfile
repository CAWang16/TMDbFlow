# Use lightweight Python base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy all files from the current directory into the container
COPY . /app

# Install dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Set the entrypoint to run source.py when the container starts
ENTRYPOINT ["python", "source.py"]
