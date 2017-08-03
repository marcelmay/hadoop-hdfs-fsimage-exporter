java -dsa -da -XX:+UseG1GC -Xmx1024m \
    -jar target/fsimage-exporter-1.0-SNAPSHOT.jar \
    localhost 7772 ./example.yml
