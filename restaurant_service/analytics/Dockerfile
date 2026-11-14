FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -q -B dependency:go-offline
COPY src/ src/
RUN --mount=type=cache,target=/root/.m2 mvn -q -B verify
FROM flink:2.2.0-scala_2.12-java17
COPY --from=build /build/target/restaurant-analytics.jar /opt/flink/usrlib/restaurant-analytics.jar
USER root
RUN mkdir -p /opt/flink/checkpoints /var/log/food /opt/flink/plugins/metrics-prometheus && chown -R flink:flink /opt/flink/checkpoints /var/log/food /opt/flink/plugins/metrics-prometheus
USER flink
