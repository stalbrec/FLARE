# FLARE

Workflow to analyse metagenomic samples.

> DAG gernated by: `snakemake --dag dot | dot -Tsvg > dag.svg`
![dag.svg](dag.svg)

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

run kraken

```bash
/home/admin/seq_tools/kraken2/kraken2 --db /home/admin/seq_tools/kraken2/kraken2_DB/ --threads 10 --paired --report ' + OutPath + SeqNo +'_k2_report.txt --output ' + OutPath + SeqNo + '_k2_output.txt ' + In1 + ' ' + In2
```

kraken2 output to krona


```bash
/home/admin/seq_tools/KronaTools-2.7/scripts/ImportTaxonomy.pl -t 5 -m 3 -o results/out_krona.html results/kraken_output.txt
```

