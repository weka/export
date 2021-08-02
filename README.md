# Weka Metrics Exporter

A Prometheus Metrics exporter for WekaFS. Gathers metrics and statistics from Weka Clusters and exposes a metrics endpoint for Prometheus to scrape.

Full Weka documentation is on http://docs.weka.io

Version 1.3.0:
Added additional scaling properties.  These are controlled with the `max_procs:` and  `max_threads_per_proc:` parameters in the `exporter:` section of the config file.   Up to max_procs subprocesses will be spawned to help scale api load, each with up to max_threads_per_proc threads.   The defaults should work for most clusters, as it will only spawn 1 process for each max_threads_per_proc hosts in the cluster.  For example, if you have 80 weka servers and 200 clients, and max_threads_per_proc is the default 100, it would spawn 3 processes (280 total hosts / 100 threads, rounded up).   One can increase the number of processes used (up to max_procs) by lowering max_threads_per_proc.  For example, the same 280 total hosts with max_threads_per_proc=50 would spawn 6 processes (280 total hosts / 50 threads, rounded up).

Conversely, small clusters do not require any additional resources.  For example, a cluster with 10 servers and 10 clients will spawn only 1 process with 20 threads.  ie: max_procs and max_thread_per_proc really are maximums.

Be sure to have about 1 core available per process for best results.

See the export.yml file for details on syntax changes.

The `node_groupsize:` added in v1.2.3 has been removed in favor of the above.  Apologies for the inconvience, but the 1.3.0 algorithm works better.

Version 1.2.3:

Some tuning tweaks, most notably the addition of `timeout:` and `node_groupsize:` to the `exporter:` section of the config file.   `timeout:` sets the API timeout period (increase it if you're getting API call timeouts.  Recommended max is 30.0 to 40.0).  `node_groupsize:` sets the number of nodes to fetch data for in any one API.  Lowering this value should shorten the time it takes to complete an API call, but will perform more API calls per data collection.   The goal in tuning these is to keep the total data collection time under 50 seconds, as Prometheus (by default) collects every 60s.

Version 1.1.0:

## What's new

Goodbye command-line args, goodbye ClusterSpecs!   This new version has all the configuration in the export.yml file!

With this new version, you'll have to run another instance for each Weka Cluster you have, rather than having one instance do all your clusters.   In practice, most customers either have only one cluster, or run multiple instances of export anyway, and handling more than one added a lot of complexity.  We've reduced the command-line args, but you can still override the configuration file name and set verbosity levels.

Some new sections have been added to the .yml file as a result of this:

```
exporter:
  listen_port: 8001
  loki_host: localhost
  loki_port: 3100
  timeout: 10.0
  node_groupsize: 100

cluster:
  hosts:
    - weka01
    - weka02
    - weka03
  auth_token_file: auth-token.json
```

The `exporter` section describes global settings for the export program itself, such as what port to listen on and where to find grafana/loki.

The `cluster` section describes the target weka cluster.   Note that with Weka v3.11.x, the auth-token.json file is required, due to us tightening security of the product.  The 'hosts' key is a list of hostnames or ip addresses.  You must specify at least one, and we recommend two or more.  There is no need to list all of the hosts in the Weka Cluster.

We highly recommend using the pre-compiled binary (in a tarball under Releases on this page - https://github.com/weka/export/releases), or the Docker Container, available on Docker Hub as `wekasolutions/export`, or better yet, use with the docker-compose configuration that is part of Weka-Mon (http://github.com/weka/weka-mon).








Version 1.0.0:


## What's new

Largely re-written and modularized. New Repo, resetting version to 1.0.0
Now using the weka api to talk to the cluster(s), rather than spawning weka commands.     
Improved logging and new verbosity levels.    
It can now gather metrics from multiple clusters     
Changed command-line arguments, particularly cluster specification (see below - removed -H option)     

## Installation

The simplest installation is to get the docker container via `docker pull weka-solutions/export:latest`

This package should be combined with our Grafana panels, available at `https://github.com/weka/weka-mon`  Follow the instructions there for simple installation.

To run outside a container:
1. Unpack tarball from Releases or clone repository.
2. Make sure you have Python 3.5+ and `pip` installed on your machine.
3. Install package dependencies: `pip install -r requirements.txt`.

## Running the pre-built Docker Container

The pre-built container is now maintained on Docker Hub.  To download it, please run:

```docker pull wekasolutions/export:latest```

If you download the pre-built docker container from this github repository, it may be loaded with:

```docker load export.tar```

Then run with the usual Docker Run command. (see below)

## Docker Run Commands

There are a variety of options when running the conatiner.

To ensure that the container has access to DNS, use the ```--network=host``` directive.  Alternatively it can be run with the -p parameter

If you do not use the ```--network=host```, then you might want to map /etc/hosts into the container with: ```--mount type=bind,source=/etc/hosts,target=/etc/hosts``` so hostnames in the cluster are resolved to ip addresses via /etc/hosts.

If you have changed the default password on the cluster, you will need to pass authentication tokens to the container with ```--mount type=bind,source=/root/.weka/,target=/weka/.weka/```.  Use the ```weka user login ``` command to generate the tokens, which by default are stored in ```~/.weka/```

To have messages logged via syslog on the docker host, use ```--mount type=bind,source=/dev/log,target=/dev/log```  On most hosts, these messages will appear in ```/var/log/messages``` 

If you would like to change the metrics gathered, you can modify the export.yml configuration file, then map that into the container with ```--mount type=bind,source=$PWD/export.yml,target=/weka/export.yml```

Clusters are specified as a list of hostnames or ip addresses, with an optional authfile (see above) like so: ```<wekahost>,<wekahost>,<wekahost>:authfile```, with the minimum being a single wekahost.

You may specify as many clusters to monitor as you wish - just list them on the command line (space-separated).  ```<wekahost>,<wekahost>,<wekahost>:authfile <wekahost>,<wekahost>,<wekahost>:authfile <wekahost>,<wekahost>,<wekahost>:authfile```

For example:

```
docker run -d --network=host \
  --mount type=bind,source=/root/.weka/,target=/weka/.weka/ \
  --mount type=bind,source=/dev/log,target=/dev/log \
  --mount type=bind,source=/etc/hosts,target=/etc/hosts \
  --mount type=bind,source=$PWD/export.yml,target=/weka/export.yml \
  wekasolutions/export -vv weka01,weka02,weka09:~/.weka/myauthfile
```

## Metrics Exported

1. Edit the export.yml file. All the metrics that can be collected are defined there. Note that the default contents are sufficient to populate the example dashboards. **Add ONLY what you need**. It does generate load on the cluster to collects these stats.
2. Uncomment any metrics you would like to gather that are commented out. Restart the exporter to start gathering those metrics.
3. To display new metrics in Grafana, add new panel(s) and query. Try to curl the data from the metrics endpoint.



## Manual Configuratrion

Configure prometheus to pull data from the Weka metrics exporter (see prometheus docs). Example Prometheus configuration:

```
# Weka
- job_name: 'weka'
  scrape_interval: 60s
  static_configs:
    - targets: ['localhost:8001']
    - targets: ['localhost:8002']
```

*Important*: Weka clusters update their performance stats once per minute, so setting Prometheus to scrape more often than every 60s is not useful.

It is recommended to be run as a daemon: ```nohup ./export <target host> &```. or better, run as a system service.

## Configuration

- `-p` port sets the base port number - Default port is 8001  (additional clusters will be on consecutive ports)
- `-h` will print the help message.

## Docker build instructions (optional)

To build this repositiory:

```
git clone <this repo>
docker build --tag wekasolutions/export .
```
or
```docker build --tag wekasolutions/export https://github.com/weka/export.git```

To save and/or load the image
```
docker save -o export.tar wekasolutions/export
docker load -i  export.tar
```
Comments, issues, etc can be reported in the repository's issues.

Note: This is provided via the GPLv3 license agreement (text in LICENSE.md).  Using/copying this code implies you accept the agreement.

Maintained by vince@weka.io
