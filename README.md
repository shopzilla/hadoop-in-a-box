#Hadoop-in-a-box

An all-in-one Hadoop suite and REPL for interactive development and learning!

## Installation

Simply clone the repository, and run:

```bash
mvn clean install
```

This will build a distributable tarball file under ```/path/to/project/distribution/target```. Untar the file using:

```bash
tar xf hadoop-in-a-box.tgz
```

Then change directories to find the executable scripts for running the different Hadoop-in-a-box runtime modes:

```bash
cd hadoop-in-a-box
```

## Runtime Modes

Hadoop-in-a-box comes with 2 different modes of use:

### Hadoop-Standalone Mode

This mode allows you to spin up an Hadoop DFS and MapReduce cluster for session-based usage--that is, the cluster will survive for the entire REPL session, but will shutdown and clean up upon exiting. This allows you to test and develop without the need of a full cluster, but is more long-lived than only using MiniMRCluster, as it allows you to interactively step through HDFS as your jobs are running.

```bash
Usage: ./hadoop-standalone [<core-site.xml OUTPUT LOCATION>] [<LOCAL HDFS LOCATION>]
```

The optional parameter ```<core-site.xml OUTPUT LOCATION>``` should be used to specify the output file where your current REPL session's core-site.xml file will be written. This is useful when interacting with your REPL session from outside software such as with Pig or Hive.

The optional parameter ```<LOCAL HDFS LOCATION>``` should be used if you would like for a part of your local file system to be automatically replicated in the newly spun-up HDFS. For example, if I have a directory: ```/Users/jl/HDFS``` that looks like:

```bash
/Users/jl/HDFS/data
/Users/jl/HDFS/data/working
/Users/jl/HDFS/data/done
/Users/jl/HDFS/data/done/good-data.txt
```

Then when I start the hadoop-standalone:

```bash
./hadoop-standalone ~/HDFS
```

The new HDFS will contain:

```bash
/data
/data/working
/data/done
/data/done/good-data.txt
```

This feature is useful for when you have M/R jobs which have a required dependency on pre-existing data artifacts such as part-of-speech files, lookup tables, etc.

Note that after the cluster is built, you are then immediately plunged into a [Hadoop REPL](#the-repl).

### Hadoop-REPL-only Mode

This mode is similar to the above, except that instead of spinning up a brand-new cluster, the REPL is pointed to an existing cluster:

```bash
Usage: ./hadoop-repl </path/to/core-site.xml>
```

After connecting to the remote cluster, you will then be started into a [Hadoop REPL](#the-repl)

### The REPL

The common piece between the above two modes of use is the custom REPL built around Hadoop.

Though there is already a CLI for HDFS and a shell script for invoking M/R jobs, these clearly have fallen short of some of today's more interactive programming utitlities. As such, the new custom Hadoop REPL has added the following features (with more to come!):
* HDFS filename tab completion/navigation
* Interactively viewing real-time changes to the underlying HDFS state

Some additional features currently under developement:
* An added "current-working-directory" concept that allows you to ```cd``` into a given directory
* M/R job controls and monitoring
* Direct HDFS file editting
* HDFS session-state saving (so you can run a job in one session, save the session, then re-run the REPL with the saved HDFS from the prior job run)
* Plenty more!

