FROM eclipse-temurin:21-jre-alpine

WORKDIR /app
COPY build/libs/simplified-data-0.1.0.jar app.jar
RUN mkdir -p /app/logs && chown 1000:1000 /app/logs

ENTRYPOINT ["java", "-jar", "app.jar"]
