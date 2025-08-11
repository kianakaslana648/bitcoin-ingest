# Use official OpenJDK 17 base image (or choose version you want)
FROM openjdk:17-jdk-alpine

# Create app directory inside container
WORKDIR /app

# Copy your fat jar into the container
COPY target/bitcoin-kafka-cronjob-1.0-SNAPSHOT.jar app.jar

# Run the jar by default when container starts
ENTRYPOINT ["java", "-jar", "app.jar"]