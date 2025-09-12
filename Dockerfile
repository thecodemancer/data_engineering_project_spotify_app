# Dockerfile

# 1. Use an official Python runtime as a parent image.
# Using 'slim' keeps the final image size smaller.
FROM python:3.9-slim

# 2. Set environment variables for the container.
# This prevents Python from writing pyc files to disc.
ENV PYTHONDONTWRITEBYTECODE 1
# This ensures Python output is sent straight to the terminal without buffering.
ENV PYTHONUNBUFFERED 1

# 3. Set the working directory in the container.
WORKDIR /app

# 4. Copy the dependencies file and install them.
# This is done in a separate step to leverage Docker's layer caching.
# If requirements.txt doesn't change, this layer is reused, speeding up builds.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy your application code into the container.
COPY . .

# 6. Define the command to run your app.
# Cloud Run automatically sets the PORT environment variable.
# This command tells Gunicorn to start, listen on that port, and run the 'app'
# object from your 'app.py' file.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]
