FROM openjdk:17-jdk-slim
ENV TRINO_VERSION=476
RUN curl -L https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz \
    | tar -xz -C /usr/local
WORKDIR /usr/local/trino-server-${TRINO_VERSION}
ENV PATH="/usr/local/trino-server-${TRINO_VERSION}/bin:$PATH"