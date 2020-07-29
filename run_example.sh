java -dsa -da -XX:+UseG1GC -Xmx1024m \
    -Dlog.level=INFO \
    -jar target/fsimage-exporter-*-SNAPSHOT.jar \
    localhost 9709 ./example.yml
