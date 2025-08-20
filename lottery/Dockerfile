FROM prefecthq/prefect:3-python3.12

# Install curl and clean up to reduce image size
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install uv

# Set working directory
WORKDIR /opt/prefect/lottery

# Copy requirements file
COPY lottery/requirements.txt .

# Install dependencies using uv pip
RUN uv pip install -r requirements.txt
