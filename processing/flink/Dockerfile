FROM flink:1.17.1-scala_2.12-java11

# Install Python 3, pip, and curl
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv curl && \
    rm -rf /var/lib/apt/lists/*

# Creating a SYMLINK
RUN ln -s /usr/bin/python3 /usr/bin/python    

# Install PyFlink (must match the Flink version)
RUN pip3 install --no-cache-dir apache-flink==1.17.1

# Download required connector JARs directly into Flink's lib directory
# This avoids volume mount shadowing issues!
RUN curl -o /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.17.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.17/flink-sql-connector-kafka-3.0.1-1.17.jar && \
    curl -o /opt/flink/lib/flink-connector-jdbc-3.1.1-1.17.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar && \
    curl -o /opt/flink/lib/postgresql-42.6.0.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# Switch back to the flink user for security
USER flink
