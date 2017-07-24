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

## Example output

### Example home output

![Home output](home.png)

### Example metrics
Here's the example output for the [test fsimage](src/test/resources/fsimage_0):

```
# HELP fsimage_file_count Number of files.
# TYPE fsimage_file_count gauge
fsimage_file_count 16.0
# HELP fsimage_scrape_requests_total Exporter requests made
# TYPE fsimage_scrape_requests_total counter
fsimage_scrape_requests_total 1.0
# HELP fsimage_user_size_bytes Sum of all file sizes.
# TYPE fsimage_user_size_bytes gauge
fsimage_user_size_bytes{user_name="foo",} 1.6777216E8
fsimage_user_size_bytes{user_name="root",} 1024.0
fsimage_user_size_bytes{user_name="mm",} 1.8863616E8
# HELP fsimage_scrape_error_total Non-zero if this scrape failed.
# TYPE fsimage_scrape_error_total gauge
fsimage_scrape_error_total 0.0
# HELP fsimage_user_fsize_bucket_count Counts file size distribution in buckets, showing small files and size distribution. Bucket label is upper size in bytes
# TYPE fsimage_user_fsize_bucket_count gauge
fsimage_user_fsize_bucket_count{user_name="mm",bucket="1048576",} 3.0
fsimage_user_fsize_bucket_count{user_name="foo",bucket="10737418240",} 0.0
fsimage_user_fsize_bucket_count{user_name="mm",bucket="134217728",} 1.0
fsimage_user_fsize_bucket_count{user_name="root",bucket="134217728",} 0.0
fsimage_user_fsize_bucket_count{user_name="foo",bucket="33554432",} 0.0
fsimage_user_fsize_bucket_count{user_name="root",bucket="1073741824",} 0.0
fsimage_user_fsize_bucket_count{user_name="root",bucket="9223372036854775807",} 0.0
fsimage_user_fsize_bucket_count{user_name="mm",bucket="0",} 0.0
fsimage_user_fsize_bucket_count{user_name="mm",bucket="1073741824",} 0.0
fsimage_user_fsize_bucket_count{user_name="mm",bucket="9223372036854775807",} 0.0
fsimage_user_fsize_bucket_count{user_name="root",bucket="33554432",} 0.0
fsimage_user_fsize_bucket_count{user_name="root",bucket="1048576",} 1.0
fsimage_user_fsize_bucket_count{user_name="foo",bucket="67108864",} 0.0
fsimage_user_fsize_bucket_count{user_name="foo",bucket="0",} 0.0
fsimage_user_fsize_bucket_count{user_name="foo",bucket="1073741824",} 1.0
fsimage_user_fsize_bucket_count{user_name="foo",bucket="9223372036854775807",} 0.0
fsimage_user_fsize_bucket_count{user_name="mm",bucket="67108864",} 1.0
fsimage_user_fsize_bucket_count{user_name="mm",bucket="10737418240",} 0.0
fsimage_user_fsize_bucket_count{user_name="foo",bucket="1048576",} 0.0
fsimage_user_fsize_bucket_count{user_name="mm",bucket="33554432",} 9.0
fsimage_user_fsize_bucket_count{user_name="root",bucket="67108864",} 0.0
fsimage_user_fsize_bucket_count{user_name="foo",bucket="134217728",} 0.0
fsimage_user_fsize_bucket_count{user_name="root",bucket="0",} 0.0
fsimage_user_fsize_bucket_count{user_name="root",bucket="10737418240",} 0.0
# HELP fsimage_scrape_duration_ms Scrape duration in ms
# TYPE fsimage_scrape_duration_ms gauge
fsimage_scrape_duration_ms 149.0
# HELP fsimage_dir_count Number of directories.
# TYPE fsimage_dir_count gauge
fsimage_dir_count 14.0
# HELP fsimage_file_size_bytes Sum of all file sizes.
# TYPE fsimage_file_size_bytes gauge
fsimage_file_size_bytes 3.56409344E8
# HELP fsimage_group_dir_count Number of directories.
# TYPE fsimage_group_dir_count gauge
fsimage_group_dir_count{group_name="root",} 0.0
fsimage_group_dir_count{group_name="supergroup",} 14.0
fsimage_group_dir_count{group_name="nobody",} 0.0
# HELP fsimage_user_file_count Number of files.
# TYPE fsimage_user_file_count gauge
fsimage_user_file_count{user_name="foo",} 1.0
fsimage_user_file_count{user_name="root",} 1.0
fsimage_user_file_count{user_name="mm",} 14.0
# HELP fsimage_user_block_count Number of blocks.
# TYPE fsimage_user_block_count gauge
fsimage_user_block_count{user_name="foo",} 2.0
fsimage_user_block_count{user_name="root",} 1.0
fsimage_user_block_count{user_name="mm",} 14.0
# HELP fsimage_block_count Number of blocks.
# TYPE fsimage_block_count gauge
fsimage_block_count 17.0
# HELP fsimage_group_file_count Number of files.
# TYPE fsimage_group_file_count gauge
fsimage_group_file_count{group_name="root",} 1.0
fsimage_group_file_count{group_name="supergroup",} 13.0
fsimage_group_file_count{group_name="nobody",} 2.0
# HELP fsimage_group_size_bytes Sum of all file sizes.
# TYPE fsimage_group_size_bytes gauge
fsimage_group_size_bytes{group_name="root",} 1024.0
fsimage_group_size_bytes{group_name="supergroup",} 1.6766464E8
fsimage_group_size_bytes{group_name="nobody",} 1.8874368E8
# HELP fsimage_user_dir_count Number of directories.
# TYPE fsimage_user_dir_count gauge
fsimage_user_dir_count{user_name="foo",} 0.0
fsimage_user_dir_count{user_name="root",} 0.0
fsimage_user_dir_count{user_name="mm",} 14.0
# HELP fsimage_exporter_app_info Application build info
# TYPE fsimage_exporter_app_info gauge
fsimage_exporter_app_info{appName="fsimage_exporter",appVersion="1.0-SNAPSHOT",buildTime="2017-07-24/20:04",buildScmVersion="68aef7af7c522e61677de8a0203b31d9da652244",buildScmBranch="master",} 1.0
# HELP fsimage_scrape_skip_total Counts the fsimage scrapes skips, as the fsimage is versioned and got already processed.
# TYPE fsimage_scrape_skip_total gauge
fsimage_scrape_skip_total 1.0
# HELP fsimage_group_block_count Number of blocks.
# TYPE fsimage_group_block_count gauge
fsimage_group_block_count{group_name="root",} 1.0
fsimage_group_block_count{group_name="supergroup",} 13.0
fsimage_group_block_count{group_name="nobody",} 3.0
```
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
