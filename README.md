# FLARE

Workflow to analyse metagenomic samples.

> DAG generated with: `snakemake --dag dot | dot -Tsvg > dag.svg`
![dag.svg](dag.svg)


## Quickstart

In order to run this workflow, follow these steps:

1. clone the repository and install base `snakemake` environment:

    ```bash
    git clone https://github.com/stalbrec/FLARE
    cd FLARE
    conda env create -n snakemake -f envs/snakemake.yaml
    ```

2. update [`config/samples.tsv`](config/samples.tsv), to your desired list of samples, e.g.:

    ```tsv
    sample	R1	R2
    Sample1	/path/to/Sample1/Sample1_R1.fastq.gz	/path/to/Sample1/Sample1_R1.fastq.gz
    ```
3. run the workflow
    ```bash
    snakemake 
    # options: 
    # --cores <number of cores default:64> 
    # -n perform a dry-run and list the jobs that will be run
    ```

#### Update Workflow

Simply change directory in the terminal to `<path/to>/FLARE` and execute `git pull`.

## Kraken2

for kraken2 rules there is the option to load the database to tmpfs. For this, make sure you have a large enough tmpfs mounted somewhere and point the option `kraken2_tmpfs_path` in [config.yml](config/config.yml#L4).
Make sure to set `kraken2_use_tmpfs` to `true`, and if you want to keep the database there for future runs, set `notep` in [profiles/default/config.yaml](profiles/default/config.yaml#L2) to `true`

## `tmpfs` for large databases

Instead of loading large databases to memory for each process separately or manually moving it to `/dev/smh` we use a dedicated `tmpfs` mounted to `/mnt/database_tmp`:

```bash
sudo mkdir /mnt/database_tmpfs
sudo mount -t tmpfs -o size=200G tmpfs /mnt/database_tmpfs
sudo chown -R root:data /mnt/database_tmpfs
```


