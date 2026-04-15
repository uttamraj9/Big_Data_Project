FROM openjdk:17-slim

# Install python3 and pip
RUN apt-get update && apt-get install -y python3 python3-pip && apt-get clean

# Set environment variables for Java
ENV JAVA_HOME=/usr/local/openjdk-17
ENV PATH=$JAVA_HOME/bin:$PATH

# Set working directory inside container
WORKDIR /app

# Copy everything from the current directory (RandomForest) into /app in container
COPY . .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Expose the port your app will run on
EXPOSE 5000

# Command to run your app
CMD ["python3", "app.py"]

