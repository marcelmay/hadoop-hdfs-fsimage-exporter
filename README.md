Prometheus Hadoop HDFS FSImage Exporter
=======

Exports Hadoop HDFS statistics to [Prometheus monitoring](https://prometheus.io/) including
* total / per user / per group
    * number of directories
    * number of files
    * file size distribution
    * number of blocks
    
The exporter parses the FSImage using the [Hadoop FSImage Analysis library](https://github.com/marcelmay/hfsa).

## Requirements
For building:
* JDK 8
* [Maven 3.5.x](http://maven.apache.org)

For running:
* JRE 8 for running
* Access to Hadoop FSImage file

## Building

```mvn clean install```

## Installation and configuration

* Install the JAR on a system where the FSImage is locally available (eg name node server).
* Configure the exporter     
  Create a yml file (see [example.yml](example.yml)):
  ```
  fsImagePath : '<path to name node fsimage_0xxx file location>'
  skipPreviouslyParsed : true
  ```
* Run the exporter
  ```
    > java -jar target/fsimage-exporter.jar
    Usage: WebServer <hostname> <port> <yml configuration file>
  ```
  Example JVM opts (-Xmx max heap depends on your fsimage size): 
  ```
  > java -Xmx1024m -dsa -server -XX:+UseG1GC \
         -jar target/fsimage-exporter-1.0-SNAPSHOT.jar \
         0.0.0.0 9092 example.yml
  ```
  Note: Make sure to size the heap correctly. As an heuristic, you can use 1.5 * fsimage size.
  
* Test the exporter  
  Open http://\<hostname>:\<port>/metrics or http://\<hostname>:\<port>/ (for configuration overview)
   
* Add to prometheus
  ```
  - job_name: 'fsimage'
      scrape_interval: 180m # Depends on how often the name node writes a fsimage file
      scrape_timeout:  200s # Depends on size
      static_configs:
        - targets: ['<hostname>:<port>']
          labels:
            ...
  ```

## Roadmap

* Release 1.0
* Export the exporter JVM heap usage, for exporter fine tuning
* Export additionally to total/user/group the stats also for a configurable list of directories
* Export symlink stats?
* Logging conf
* Docker image
* Example Grafana dashboard?

## License

This Hadoop HDFS FSImage Exporter is released under the [Apache 2.0 license](LICENSE).

```
Copyright 2017 Marcel May  

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
