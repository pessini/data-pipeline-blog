# Use Python 3.12 slim image as base
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install uv
RUN pip install uv

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies using uv
RUN uv pip install --system -r requirements.txt

# Copy application code
COPY app/ ./app/

# Run Streamlit
ENTRYPOINT ["streamlit", "run", "app/main.py", "--server.port=17113", "--server.address=0.0.0.0"]
