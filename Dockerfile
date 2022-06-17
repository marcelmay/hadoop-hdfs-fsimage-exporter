# Hadoop FSImage Exporter
#
# Ports:
# - 9709 : The port exposing exporter metrics
# Volumes:
# - /fsimage-location : The expected mount containing FSIMAGE files
#
# Example usage:
# > docker build -t example-fsimage-exporter -f Dockerfile .
# > docker run -it -p 9709:9709 -v $PWD/src/test/resources:/fsimage-location example-fsimage-exporter
#
FROM azul/zulu-openjdk-alpine:17.0.3-jre-headless

LABEL org.opencontainers.image.url=https://github.com/marcelmay/hadoop-hdfs-fsimage-exporter
LABEL org.opencontainers.image.source=https://github.com/marcelmay/hadoop-hdfs-fsimage-exporter/blob/master/Dockerfile

EXPOSE 9709
ARG APP_USER=fsimage_exporter
ARG APP_USER_ID=1000
ARG APP_HOME=/opt/fsimage-exporter

ENV JAVA_OPTS -server
ENV FSIMAGE_EXPORTER_OPTS 0.0.0.0 9709 $APP_HOME/fsimage-exporter-config.yml
ENV APP_HOME=$APP_HOME

# Install dumb-init
RUN apk add --no-cache --update ca-certificates dumb-init

# Create non-root user
RUN adduser --home $APP_HOME --uid 1000 --disabled-password --shell /usr/sbin/nologin $APP_USER  
USER $APP_USER_ID

# Copy artifacts
COPY --chown=$APP_USER_ID:$APP_USER_ID ./fsimage-docker.yml $APP_HOME/fsimage-exporter-config.yml
COPY --chown=$APP_USER_ID:$APP_USER_ID ./target/fsimage-exporter-*.jar $APP_HOME/fsimage-exporter.jar

# Run
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/bin/sh", "-c", \
    "echo JAVA_OPTS=\"$JAVA_OPTS\" && \
     echo FSIMAGE_EXPORTER_OPTS=\"$FSIMAGE_EXPORTER_OPTS\" && \
     echo UID=`id` && \
     echo CMD=\"java $JAVA_OPTS -jar $APP_HOME/fsimage-exporter.jar $FSIMAGE_EXPORTER_OPTS\" && \
     exec java $JAVA_OPTS -jar $APP_HOME/fsimage-exporter.jar $FSIMAGE_EXPORTER_OPTS"]
