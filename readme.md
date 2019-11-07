# Kafka Suite

The command line tool to help manage administrative Kafka issues, like partition balancing. Uses profile approach, so just store Kafka cluster settings once and switch between different clusters. 

## Getting started

(TODO)

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

All modules in this pseudo-group are doing partition reassignment. 

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

If the cluster is in a good health but for any reason you want to replace the broker with another one, just first make sure it's on the cluster and operating well and then move all partitions from the broker you want 

## Questions?

If you find any issue with the toolset, or something is not clear in documentation, or you have a suggestion, feel free to create an issue.

## TODOs

* stopping reassignment automation:
    - remove /admin/reassignment
    - clean up partitions in /brokers/topics/$topic -- should be the same as ... ???
* `reassignment --wait` to update all data, not only reassignment
* cluster info:
    * racks
    * who is controller
* consumers
    - list
    - change offset