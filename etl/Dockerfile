# Use an official Python runtime as a parent image
FROM python:3.6

# Set the working directory to /etl
WORKDIR /etl

# Copy the requirements file into the container at /etl
COPY requirements.txt /etl

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run etl.py when the container launches
CMD ["python", "etl.py"]