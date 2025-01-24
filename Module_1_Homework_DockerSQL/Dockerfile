FROM python:3.12.8

# Install necessary dependencies
RUN pip install pyarrow
RUN apt-get install curl
RUN pip install pandas sqlalchemy psycopg2 requests

# Set the working directory
WORKDIR /app

# Copy your Python scripts into the container
COPY ingest_data.py ingest_data.py 


# Use the wrapper script as the entrypoint
ENTRYPOINT ["python", "ingest_data.py"]