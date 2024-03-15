FROM python:3.11-slim

# Set the working directory

WORKDIR /app

# install requirements

COPY ./requirements.txt .

# Install git, install dependencies from requirements.txt, then remove git to save space
RUN apt-get update && \
    apt-get install -y git && \
    pip install -r requirements.txt && \
    apt-get remove -y git && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy rest of app

COPY ./app .

CMD ["python", "app.py"]
