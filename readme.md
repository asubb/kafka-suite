# Kafka Suite

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  

  - [Getting started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
    - [Compilation from sources](#compilation-from-sources)
      - [IDE](#ide)
  - [Kafka Suite modules](#kafka-suite-modules)
    - [Reassignment module group](#reassignment-module-group)
      - [Replace node in Kafka custer](#replace-node-in-kafka-custer)
      - [Replace absent node in Kafka cluster](#replace-absent-node-in-kafka-cluster)
      - [Fix No Leader](#fix-no-leader)
      - [Reassignments tracking](#reassignments-tracking)
    - [Profiles](#profiles)
      - [Weight Functions](#weight-functions)
    - [Analyze module](#analyze-module)
    - [Info module](#info-module)
  - [Kafka Suite recipes](#kafka-suite-recipes)
    - [If someone killed brokers. A few of them.](#if-someone-killed-brokers-a-few-of-them)
    - [Changing replication factor](#changing-replication-factor)
    - [Replacing node in the cluster.](#replacing-node-in-the-cluster)
    - [Replacing absent node in the cluster.](#replacing-absent-node-in-the-cluster)
  - [Questions?](#questions)
  - [TODOs](#todos)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

The command line tool to help manage administrative Kafka issues, like partition balancing. Uses profile approach, so just store Kafka cluster settings once and switch between different clusters.

Current version is `0.2.1` 

## Getting started

### Prerequisites
 
 * JRE 8 should be present and installed correctly.
 * ZooKeeper cluster should be accessible via direct connection.
 
### Installation

* download binaries: [tar](https://github.com/asubb/kafka-suite/releases/download/0.2.1/ksuite-0.2.1.tar) or [zip](https://github.com/asubb/kafka-suite/releases/download/0.2.1/ksuite-0.2.1.zip)
* unpack it: 
    
    ```bash
    tar -xzf ksuite-0.2.1.tar
    ```
    
    or
    
    ```bash
    unzip ksuite-0.2.1.zip
    ```
* start using it: 
    
    ```bash
    cd ksuite-0.2.1/bin
    ./ksuite
    ```

### Compilation from sources

In order to get the version from source you would need to have JDK 8 installed. Project uses Gradle build system with the wrapper, so you won't need to install anything extra, it'll download it.

* Compile and run tests:

    ```bash
     ./gradlew build
    ```

* Tar distribution

    ```bash
    ./gradlew distTar
    ```

* Zip distribution

    ```bash
    ./gradlew distZip
    ```

* Unpacked distributions

    ```bash
    ./gradlew installDist
    ```

#### IDE

The Intellij IDE was used to develop the project. Inorder to set it up just open the `build.gradle` file in the IDE and add it as gradle project. That'll do.  

Useful run configurations:
1. Main program: `Run configurations > Add new > Kotlin`, populate `Main Class` with `kafka.suite.KafkaSuiteKt`, provide command argument in `Program arguments` field
2. Run all tests: `Run configurations > Add new > JUnit`, populate `Test kind` with `All in package` and package `kafka.suite`

## Kafka Suite modules

Suite consists of different modules, each of them aims to solve specific problem. However, module can't solve complex problems and sometimes you need to apply a few modules sequentially. Follow [recipes section](#kafka-suite-recipes). 

In order to run a module but it on the first parameter position:

```bash
ksuite module-name --module-parameter value
```


* Recommended to make a dry run (`--dry-run` or `-d`) before running any module to make sure module is doing exactly what you expect:
    ```bash
    ksuite replace-node -r 1001 -s 1002 --dry-run
    ```
* In order to help about modules or any specific module use `--help` or `-h` flag (default action if not specified anything):
    To get general help
    ```bash
    ksuite --help
    ```
    To get module help
    ```bash
    ksuite replace-node --help 
    ```
* If something is wrong and you want dive deeper into investigation, enable `--debug` flag. It will output to console all DEBUG level logs from app as well as from all dependencies, including Kafka and Zookeeper Client.

### Reassignment module group

All modules in this pseudo-group are doing partition reassignment. Most of the modules use weight function, so make sure you set it up properly, the default one is MONO which treats all partitions the same. Also worth to mention, all partition planning is rack-aware.

#### Replace node in Kafka custer

Moves partitions from one broker to another. Both brokers assumed are available and healthy. Consider using when you need to replace broker do to some maintenance or underlying hardware degradation (common case when you run cluster for example on AWS).

#### Replace absent node in Kafka cluster

When one or more nodes became unavailable in Kafka cluster, the partition become under replicated. To fix that issue you need assign partition replicas to another node. This module detects such nodes and suggest them to spread across the cluster.

Notes:
* partition assignment is performed using [weight function](#weight-functions)
* for some versions of Kafka the partition assignment may stuck. Use [forced reassignment](#reassignments-tracking) (with caution!)

#### Fix No Leader

Fixes no leader on under replicated partition by assigning it to a different broker keeping the same replication factor or overriding with another one. 

#### Reassignments tracking

Module covers different reassignment-in-progress issues. Kafka may have only one reassignment at a time, so in order to wait till reassignment finishes you may use this module. In case the job is stuck, it kicks up partitions (`--force` flag).

### Profiles

Profiles allows to keep configuration and reuse it between the runs. You may have as many profiles as you need, just keep them named uniquely.
The profile is kept in json format in your home folder: `~/.ksuite.json`

Profile keep the following information

| Field name | Description           | 
|------------|-----------------------|
| name       | The name or a key of the profile |
| active     | If the profile is activated. Only one can be active. |
| brokers    | Kafka broker list                |
| zookeeper  | Zookeeper connection string      |
| kafkaBin   | For some clusters may need to connect via their Kafka Cli client. Specify the path to Kafka binaries of that is so. |
| weights    | Object describes weights for partition balancing. See [weight](#weight-functions) for details |
| zkClient   | Whether to use ZK Client to get information about the cluster (i.e. Kafka 0.10 may use only that approach) |

To add/edit cluster just specify a name and required parameters. If cluster is not found the new one will be created. This profile automatically becomes active.
```bash
ksuite profle --name NEW_PROFILE --zookeeper zoo1:2181,zoo2:2182 --brokers kafka1:2181,kafka2:2181
```

```bash
ksuite profle --name EXISTING_PROFILE
```


#### Weight Functions

With weight functions you may specify what weight has specific partition to evenly spread them across different brokers. Currently two weight functions are supported:

1. Profile Based. It uses "credits" for each of data points, i.e. CPU load, memory and so on, the function just sums them up and that becomes a weight of the partition.
2. Mono functions. Just assumes that all partitions are the same. Default behavior.

How exactly the weight is used depends on the operation you perform. However the main idea behind is that it tries to keep the same weight on each node.

### Analyze module

Makes an attempt to check if some of the topics need attention. Detects if partition has NO LEADER, UNDER REPLICATED, or if there is an ABSENT_BROKER.

### Info module

Prints the information regarding your cluster.

## Kafka Suite recipes 

Tool by itself provides certain operation you can simplify doing with the cluster, however the problems usually are much harder to resolve by just running one command, so here is a collection of problems and possible solution that Kafka Suite may help resolve. ** Please apply it at your own risk. The tool just automates routine but it doesn't remove the need of understanding what's going on. **
Examples are done using `zsh` running on MacOS, so it may not work for your environment.

### If someone killed brokers. A few of them.

If by any reason a few nodes disappeared from the cluster or became malfunctioning, despite the fact the partition would be under replicated, some of them may result in NO LEADER state. That usually means that data is being lost anyway. The next thing, most likely you won't have enough nodes to spread the partitions on (depends). So the idea behind this recipe is to reduce replication factor to 1 for all partitions and restore the working state of the cluster (yes, fragile, but working). Later on you would need to return back your desired replication factor, but first you would need to restore lost nodes. Worth to mention you may need to kick reassignment up, follow [forced reassignment](#reassignments-tracking) (with caution!)

```bash
TOPIC=${1:-<NO_TOPIC>}  # topic name, just one at once 
DRY_RUN=$2              # specify -d or --dry-run
WAIT_INTERVAL=5         # interval between checks
HOME=ksuite/bin         # path to ksuite execution file

echo ">>> NO LEADER FIX"
$HOME/ksuite fix-no-leader $DRY_RUN --topics $TOPIC --replication-factor 1
$HOME/ksuite reassignment --wait $WAIT_INTERVAL
echo ">>> RF=1"
$HOME/ksuite change-replication-factor $DRY_RUN --topics $TOPIC --replication-factor 1 --skip-no-leader --isr-based
$HOME/ksuite reassignment --wait $WAIT_INTERVAL
```

### Changing replication factor

Assuming the cluster is in healthy state and you can perform any operation, that recipe is basically one-command. The tool automatically figure out of the replication factor should be increased or decreased.

```bash
TOPIC=${1:-<NO_TOPIC>}  # topic name, just one at once 
DRY_RUN=$2              # specify -d or --dry-run
WAIT_INTERVAL=5         # interval between checks
HOME=ksuite/bin         # path to ksuite execution file
RF=3                    # replication factor to set

$HOME/ksuite change-replication-factor $DRY_RUN --topics $TOPIC --replication-factor $RF
$HOME/ksuite reassignment --wait $WAIT_INTERVAL
```

### Replacing node in the cluster.

If the cluster is in a good health but for any reason you want to replace the broker with another one, just first make sure it's on the cluster and operating well and then move all partitions from the broker you want. 

```bash
R=$1                    # broker ID to replace 
S=$2                    # broker ID to replace with 
DRY_RUN=$3              # specify -d or --dry-run
WAIT_INTERVAL=5         # interval between checks
HOME=ksuite/bin         # path to ksuite execution file

$HOME/ksuite replace-node $DRY_RUN --replacing $R --substitution $S 
$HOME/ksuite reassignment --wait $WAIT_INTERVAL
```

### Replacing absent node in the cluster.

If the node is absent, there is a way to detect it automatically and spread the partitions across the cluster. Uses weight function for choosing node to move partition to. Sometimes it may require to kick up the reassignment.

```bash
DRY_RUN=$1              # specify -d or --dry-run
WAIT_INTERVAL=5         # interval between checks
HOME=ksuite/bin         # path to ksuite execution file

$HOME/ksuite replace-absent-node $DRY_RUN 
$HOME/ksuite reassignment --wait $WAIT_INTERVAL
```

### Rebalancing cluster load with replication factor trick

Let's assume the cluster is not balanced well and different nodes having troubles serving all traffic. The reason might be that a lot of heavy partitions are on the same nodes. Also for that recipe to work you need to have at least replication factor of 2.

The idea here is to use proper Weight function, so you need to spend some time [tuning it](#weight-functions) for you [profile](#profiles) and make sure it looks exactly as in your monitoring job, look into [info module](#info-module).

When it is ready, the idea is to decrease replication factor all the way to 1 and then back to desired replication factor. That doesn't solve problem absolutely right as still may have problems if non-replicated partitions are not spread efficiently, but that still is a viable solution for most cases. 

One important note, while change replication back to desired value, it is essential to tell that **profile weight function** should be used, otherwise you'll get unbalanced cluster. Also it is recommended to increase replication factor slowly so your cluster space would perfectly fit the desired volume. 

```bash
DRY_RUN=$1              # specify -d or --dry-run
WAIT_INTERVAL=5         # interval between checks
HOME=ksuite/bin         # path to ksuite execution file
RF=3                    # final replication factor

# decrease replication to 1. Should be fairly fast.
$HOME/ksuite change-replication-factor $DRY_RUN --replication-factor 1
$HOME/ksuite reassignment --wait $WAIT_INTERVAL

# increase replication back to desired value. Will take some time depending on your cluster size.
$HOME/ksuite change-replication-factor $DRY_RUN --replication-factor $RF --weightFn profile
$HOME/ksuite reassignment --wait $WAIT_INTERVAL
```


## Questions?

If you find any issue with the toolset, or something is not clear in documentation, or you have a suggestion, feel free to create an issue.

## TODOs

* `reassignment --wait` to update all data, not only reassignment
* cluster info:
    * who is controller
* consumers
    - list
    - change offset