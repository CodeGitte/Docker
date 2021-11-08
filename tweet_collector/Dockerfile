# Use an official Python runtime as a parent image
FROM python:3.6

# Set the working directory to /tweets
WORKDIR /tweets

# Copy the requirements file into the container at /app
# (a text files with all the libraries you want to install)
COPY requirements.txt /tweets

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run get_tweets.py when the container launches
CMD ["python", "collect_tweets.py"]