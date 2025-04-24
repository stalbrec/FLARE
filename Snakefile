import pandas as pd
configfile: "config/config.yml"
SAMPLES_DF = pd.read_csv(config["samples"], sep="\t")
SAMPLES = SAMPLES_DF["sample"].to_list()
SAMPLE_READS = {
    row["sample"]: {k:row[k] for k in ["R1","R2"]} for _,row in SAMPLES_DF.iterrows()
}

rule all:
    input:
        "test/test.dat",
        expand("results/{sample}/sample_report.md",sample=SAMPLES)

rule sample_report:
    input:
        kraken_result_fp = "results/{sample}/kraken2/output.txt"
    output:
        report_fp = "results/{sample}/sample_report.md"
    shell:
        """
        touch {output.report_fp} 
        """

# might be useful if your db is on a slow disk and you have plenty of free memory:
rule prepare_kraken2_db_shm:
    input:
        db_dir = config["kraken2_db"]
    output:
        db_dir = temp(directory(config["kraken2_tmpfs_path"]))
    log:
        "logs/kraken2_db_setup.log"
    shell:
        """
        set -euo pipefail
        echo "[INFO] $(date) - setup of kraken2 db" > {log}
        if [[ "{config[kraken2_use_tmpfs]}" == "True" ]]; then
            echo "[INFO] $(date) - attempt to copy kraken2 db into tmpfs ({output.db_dir})..." >> {log}
            KRAKEN_DB={output.db_dir}
            if [[ -d $KRAKEN_DB ]]; then
                echo "[INFO] $(date) - kraken2-db already in shared memory" >> {log}
            else
                echo "[INFO] $(date) - copying kraken2-db..." >> {log}
                mkdir -p $KRAKEN_DB
                rsync --info=progress2 {config[kraken2_db]}/*.k2d $KRAKEN_DB >> {log}
            fi
        fi
        """

rule run_kraken2:
    input:
        R1 = lambda wc: SAMPLE_READS[wc.sample]["R1"],
        R2 = lambda wc: SAMPLE_READS[wc.sample]["R2"],
        kraken2_db = rules.prepare_kraken2_db_shm.output.db_dir if config["kraken2_use_tmpfs"] else config["kraken2_db"]
    output:
        kraken_output_fp="results/{sample}/kraken2/output.txt",
        kraken_report_fp="results/{sample}/kraken2/report.txt"
    conda:
        "envs/metagen.yaml"
    log:
        "logs/{sample}/kraken2.log"
    shell:
        """
        EXTRA_ARGS=""
        KRAKEN_DB={input.kraken2_db}
        echo "[INFO] $(date) - Running kraken2 on samples: \n{input.R1}\n{input.R2}" > {log}
        if [[ "{config[kraken2_use_tmpfs]}" == "True" || "{config[kraken2_memory_mapping]}" == "True" ]]; then
            echo "[INFO] $(date) - Extending kraken2 arguments with --memory-mapping." >> {log}
            EXTRA_ARGS="${{EXTRA_ARGS}} --memory-mapping "
        fi
        (\
        kraken2 --db ${{KRAKEN_DB}} --threads {threads} ${{EXTRA_ARGS}} --paired --report {output.kraken_report_fp} \
                --output {output.kraken_output_fp} {input.R1} {input.R2} \
        ) 2>> {log}
        """