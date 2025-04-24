import pandas as pd
configfile: "config/config.yaml"
SAMPLES_DF = pd.read_csv(config["samples"], sep="\t")
SAMPLES = SAMPLES_DF["sample"].to_list()
SAMPLE_READS = {
    row["sample"]: {k:row[k] for k in ["R1","R2"]} for _,row in SAMPLES_DF.iterrows()
}

rule all:
    input:
        expand("results/{sample}/sample_report.md",sample=SAMPLES)

rule sample_report:
    input:
        kma_result_fp="results/{sample}/kma/{sample}_out_kma.res"
        # kraken_result_fp = "results/{sample}/kraken2/{sample}_kraken2_output.txt",
        # krona_result_fp = "results/{sample}/krona/{sample}_krona.html"
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
        "logs/databases/kraken2_setup.log"
    shell:
        """
        set -euo pipefail
        echo "[INFO] $(date) [kraken2 db] - setup of kraken2 db" > {log}
        if [[ "{config[kraken2_use_tmpfs]}" == "True" ]]; then
            echo "[INFO] $(date) [kraken2 db] - attempt to copy kraken2 db into tmpfs ({output.db_dir})..." >> {log}
            KRAKEN_DB={output.db_dir}
            if [[ -d $KRAKEN_DB ]]; then
                echo "[INFO] $(date) [kraken2 db] - kraken2-db already in shared memory" >> {log}
            else
                echo "[INFO] $(date) [kraken2 db] - copying kraken2-db..." >> {log}
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
        kraken_output_fp="results/{sample}/kraken2/{sample}_kraken2_output.txt",
        kraken_report_fp="results/{sample}/kraken2/{sample}_kraken2_report.txt"
    conda:
        "envs/metagen.yaml"
    log:
        "logs/{sample}/kraken2.log"
    shell:
        """
        EXTRA_ARGS=""
        KRAKEN_DB={input.kraken2_db}
        echo "[INFO] $(date) [kraken2] - Running kraken2 on samples: \n{input.R1}\n{input.R2}" > {log}
        if [[ "{config[kraken2_use_tmpfs]}" == "True" || "{config[kraken2_memory_mapping]}" == "True" ]]; then
            echo "[INFO] $(date) [kraken2] - Extending kraken2 arguments with --memory-mapping." >> {log}
            EXTRA_ARGS="${{EXTRA_ARGS}} --memory-mapping "
        fi
        (\
        kraken2 --db ${{KRAKEN_DB}} --threads {threads} ${{EXTRA_ARGS}} --paired --report {output.kraken_report_fp} \
                --output {output.kraken_output_fp} {input.R1} {input.R2} \
        ) >> {log} 2>&1
        """

rule run_krona:
    input: 
        kraken_output_fp="results/{sample}/kraken2/{sample}_kraken2_output.txt"
    output:
        krona_output_fp="results/{sample}/krona/{sample}_krona.html"
    conda:
        "envs/metagen.yaml"
    log:
        "logs/{sample}/krona.log"
    shell:
        """
        echo "[INFO] $(date) [krona] - Running krona on kraken2 results {input.kraken_output_fp}" > {log}
        echo "[INFO] $(date) [krona] - Checking for updates of taxonomy" >> {log}
        (ktUpdateTaxonomy.sh) >> {log} 2>&1
        
        echo "[INFO] $(date) [krona] - Running ktImportTaxonomy..." >> {log}
        (ktImportTaxonomy -t 5 -m 3 -o {output.krona_output_fp} {input.kraken_output_fp}) >> {log} 2>&1
        """

rule prepare_kma_db:
    input:
        kma_compb_fp=lambda wc: f"{config['kma_ref_database']}.comp.b"
    output:
        kma_db_is_ready_flag="results/kma_database/db_is_ready"
    log:
        "logs/database/kma_setup.log"
    conda:
        "envs/metagen.yaml"
    shell:
        """
        echo "[INFO] $(date) [kma db] - Preparing kma database." > {log}
        if [[ "{config[kma_use_shm]}" == "True" ]]; then
            echo "[INFO] $(date) [kma db] - loading database into shared memory..." >> {log}
            (kma shm -t_db {config[kma_ref_database]} -shmLvl 1) >> {log} 2>&1
        else
            echo "[INFO] $(date) [kma db] - not using shared memory." >> {log}
        fi

        touch {output.kma_db_is_ready_flag}
        """
rule cleanup_databases:
    input:
        kma_db_is_ready_flag="results/kma_database/db_is_ready",
    output:
        kma_db_cleanup_done="results/kma_database/db_cleanup_done"
    conda:
        "envs/metagen.yaml"
    log:
        "logs/databases/cleanup.log"
    shell:
        """
        echo "[INFO] $(date) [database cleanup] - Starting cleanup of databases..." > {log}
        if [[ "{config[kma_use_shm]}" == "True" ]]; then
            echo "[INFO] $(date) [database cleanup] - removing kma database ({config[kma_ref_database]}) from shared memory..." >> {log}
            (kma shm -t_db {config[kma_ref_database]} -shmLvl 1 -destroy) >> {log} 2>&1
        else
            echo "[INFO] $(date) [database cleanup] - This workflow dod not use shared memory." >> {log}
        fi
        touch {output.kma_db_cleanup_done}

        """

rule run_kma:
    input:
        R1 = lambda wc: SAMPLE_READS[wc.sample]["R1"],
        R2 = lambda wc: SAMPLE_READS[wc.sample]["R2"],
        kma_db_is_ready = "results/kma_database/db_is_ready"
    output:
        kma_align_output_aln_fp="results/{sample}/kma/{sample}_out_kma.aln",
        kma_align_output_fsa_fp="results/{sample}/kma/{sample}_out_kma.fsa",
        kma_align_output_mapstat_fp="results/{sample}/kma/{sample}_out_kma.mapstat",
        kma_align_output_res_fp="results/{sample}/kma/{sample}_out_kma.res"
    params:
        kma_align_output_prefix="results/{sample}/kma/{sample}_out_kma"
    conda:
        "envs/metagen.yaml"
    log:
        "logs/{sample}/kma.log"
    shell:
        """
        echo "[INFO] $(date) [kma] - Running kma alignment against {config[kma_ref_database]}" > {log}
        echo "[INFO] $(date) [kma] - samples: \n{input.R1}\n{input.R2}" > {log}
        EXTRA_ARGS=""
        if [[ "{config[kma_use_shm]}" == "True" ]]; then
            echo "[INFO] $(date) - Extending kma arguments with -shm." >> {log}
            EXTRA_ARGS="${{EXTRA_ARGS}} -shm "
        fi
        (kma -ipe {input.R1} {input.R2} -o {params.kma_align_output_prefix} -t_db {config[kma_ref_database]} -tmp -mem_mode ${{EXTRA_ARGS}} -ef -cge -nf -t 20) >> {log} 2>&1
        """
