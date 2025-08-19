FROM prefecthq/prefect:3-python3.12

# Install curl and clean up to reduce image size
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install uv

# Copy requirements file
COPY requirements.txt .

# Install dependencies using uv pip
RUN uv pip install -r requirements.txt

# Set working directory
WORKDIR /opt/prefect/lottery/

# Copy the lottery application code
COPY . .