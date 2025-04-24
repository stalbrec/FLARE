from snakemake.utils import min_version

min_version("5.14.0")
import pandas as pd


configfile: "config/config.yaml"


SAMPLES_DF = pd.read_csv(config["samples"], sep="\t")
SAMPLES = SAMPLES_DF["sample"].to_list()
SAMPLE_READS = {
    row["sample"]: {k: row[k] for k in ["R1", "R2"]} for _, row in SAMPLES_DF.iterrows()
}


rule all:
    input:
        expand("results/{sample}/sample_report.md", sample=SAMPLES),


rule unzip_reads:
    input:
        R1=lambda wc: SAMPLE_READS[wc.sample]["R1"],
        R2=lambda wc: SAMPLE_READS[wc.sample]["R2"],
    output:
        R1_unzipped=temp("results/raw_reads/{sample}_R1.fastq"),
        R2_unzipped=temp("results/raw_reads/{sample}_R2.fastq"),
    log:
        "logs/{sample}/raw_reads.log",
    conda:
        "envs/metagen.yaml"
    shell:
        """
        if [[ {input.R1} == *.gz ]]; then
            gunzip -c {input.R1} > {output.R1_unzipped}
        else
            cp {input.R1} {output.R1_unzipped}
        fi

        if [[ {input.R2} == *.gz ]]; then
            gunzip -c {input.R2} > {output.R2_unzipped}
        else
            cp {input.R2} {output.R2_unzipped}
        fi
        """


rule run_fastqc:
    input:
        R1=rules.unzip_reads.output.R1_unzipped,
        R2=rules.unzip_reads.output.R2_unzipped,
    output:
        fastqc_output_dir=directory("results/{sample}/{sample}_fastqc"),
    log:
        "logs/{sample}/fastqc.log",
    conda:
        "envs/metagen.yaml"
    shell:
        """
        echo "[INFO] $(date) [fastqc] Running FASTQC on: \n {input.R1} \n {input.R2}" >> {log}
        mkdir -p {output.fastqc_output_dir}
        fastqc -o {output.fastqc_output_dir} -f fastq {input.R1} {input.R2} >> {log} 2>&1
        """


rule run_multiqc:
    input:
        scan_dirs=expand(rules.run_fastqc.output.fastqc_output_dir, sample=SAMPLES),
    output:
        multiqc_report_dir=directory("results/multiqc"),
        multiqc_report_fp="results/multiqc/multiqc_report.html",
        multiqc_data_dir=directory("results/multiqc/multiqc_data"),
    log:
        "logs/multiqc.log",
    conda:
        "envs/metagen.yaml"
    shell:
        """
        echo "[INFO] $(date) [multiqc] - Running multiqc to summarize all reports" > {log}
        multiqc {input.scan_dirs} -o {output.multiqc_report_dir} >> {log} 2>&1
        #OPENAI_API_KEY=token multiqc S*/*_fastqc/  --ai --ai-custom-endpoint http://localhost:11434/v1/chat/completions --ai-model llama3.3:latest --ai-provider custom 
        """


# might be useful if your db is on a slow disk and you have plenty of free memory:
rule prepare_kraken2_db_shm:
    input:
        db_dir=config["kraken2_db"],
    output:
        db_dir=temp(directory(config["kraken2_tmpfs_path"])),
    log:
        "logs/databases/kraken2_setup.log",
    conda:
        "envs/metagen.yaml"
    params:
        use_tmpfs=config["kraken2_use_tmpfs"]
    shell:
        """
        set -euo pipefail
        echo "[INFO] $(date) [kraken2 db] - setup of kraken2 db" > {log}
        if [[ "{params.use_tmpfs}" == "True" ]]; then
            echo "[INFO] $(date) [kraken2 db] - attempt to copy kraken2 db into tmpfs ({output.db_dir})..." >> {log}
            KRAKEN_DB={output.db_dir}
            if [[ -d $KRAKEN_DB ]]; then
                echo "[INFO] $(date) [kraken2 db] - kraken2-db already in shared memory" >> {log}
            else
                echo "[INFO] $(date) [kraken2 db] - copying kraken2-db..." >> {log}
                mkdir -p $KRAKEN_DB
                rsync --info=progress2 {input.db_dir}/*.k2d $KRAKEN_DB >> {log}
            fi
        fi
        """


rule run_kraken2:
    input:
        R1=lambda wc: SAMPLE_READS[wc.sample]["R1"],
        R2=lambda wc: SAMPLE_READS[wc.sample]["R2"],
        kraken2_db=(
            rules.prepare_kraken2_db_shm.output.db_dir
            if config["kraken2_use_tmpfs"]
            else config["kraken2_db"]
        ),
    output:
        kraken_output_fp="results/{sample}/kraken2/{sample}_kraken2_output.txt",
        kraken_report_fp="results/{sample}/kraken2/{sample}_kraken2_report.txt",
    log:
        "logs/{sample}/kraken2.log",
    conda:
        "envs/metagen.yaml"
    params:
        use_tmpfs=config["kraken2_use_tmpfs"],
        use_memory_mapping=config["kraken2_memory_mapping"]
    shell:
        """
        EXTRA_ARGS=""
        KRAKEN_DB={input.kraken2_db}
        echo "[INFO] $(date) [kraken2] - Running kraken2 on samples: \n{input.R1}\n{input.R2}" > {log}
        if [[ "{params.use_tmpfs}" == "True" || "{params.use_memory_mapping}" == "True" ]]; then
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
        kraken_output_fp="results/{sample}/kraken2/{sample}_kraken2_output.txt",
    output:
        krona_output_fp="results/{sample}/krona/{sample}_krona.html",
    conda:
        "envs/metagen.yaml"
    log:
        "logs/{sample}/krona.log",
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
        kma_compb_fp=lambda wc: f"{config['kma_ref_database']}.comp.b",
    output:
        kma_db_is_ready_flag="results/kma_database/db_is_ready",
    log:
        "logs/database/kma_setup.log",
    conda:
        "envs/metagen.yaml"
    params:
        use_shm=config["kma_use_shm"],
        kma_db_local_path=config["kma_ref_database"]
    shell:
        """
        echo "[INFO] $(date) [kma db] - Preparing kma database." > {log}
        if [[ "{params.use_shm}" == "True" ]]; then
            echo "[INFO] $(date) [kma db] - loading database into shared memory..." >> {log}
            (kma shm -t_db {params.kma_db_local_path} -shmLvl 1) >> {log} 2>&1
        else
            echo "[INFO] $(date) [kma db] - not using shared memory." >> {log}
        fi

        touch {output.kma_db_is_ready_flag}
        """


rule cleanup_databases:
    input:
        kma_db_is_ready_flag="results/kma_database/db_is_ready",
    output:
        kma_db_cleanup_done="results/kma_database/db_cleanup_done",
    log:
        "logs/databases/cleanup.log"
    conda:
        "envs/metagen.yaml"
    params:
        use_shm=config["kma_use_shm"],
        kma_db_local_path=config["kma_ref_database"]

    shell:
        """
        echo "[INFO] $(date) [database cleanup] - Starting cleanup of databases..." > {log}
        if [[ "{params.use_shm}" == "True" ]]; then
            echo "[INFO] $(date) [database cleanup] - removing kma database ({params.kma_db_local_path}) from shared memory..." >> {log}
            (kma shm -t_db {params.kma_db_local_path} -shmLvl 1 -destroy) >> {log} 2>&1
        else
            echo "[INFO] $(date) [database cleanup] - This workflow dod not use shared memory." >> {log}
        fi
        touch {output.kma_db_cleanup_done}

        """


rule run_kma:
    input:
        R1=lambda wc: SAMPLE_READS[wc.sample]["R1"],
        R2=lambda wc: SAMPLE_READS[wc.sample]["R2"],
        kma_db_is_ready="results/kma_database/db_is_ready",
    output:
        kma_align_output_res_fp="results/{sample}/kma/{sample}_out_kma.res",
    params:
        kma_align_output_prefix=lambda wc, output: output["kma_align_output_res_fp"].replace(".res",""),
        use_shm=config["kma_use_shm"],
        kma_db_local_path=config["kma_ref_database"]
    conda:
        "envs/metagen.yaml"
    log:
        "logs/{sample}/kma.log",
    shell:
        """
        echo "[INFO] $(date) [kma] - Running kma alignment against {params.kma_db_local_path}" > {log}
        echo "[INFO] $(date) [kma] - samples: \n{input.R1}\n{input.R2}" > {log}
        EXTRA_ARGS=""
        if [[ "{params.use_shm}" == "True" ]]; then
            echo "[INFO] $(date) - Extending kma arguments with -shm." >> {log}
            EXTRA_ARGS="${{EXTRA_ARGS}} -shm "
        fi
        (kma -ipe {input.R1} {input.R2} -o {params.kma_align_output_prefix} -t_db {params.kma_db_local_path} -tmp -mem_mode ${{EXTRA_ARGS}} -ef -cge -nf -t 20) >> {log} 2>&1
        """


rule run_ccmetagen:
    input:
        kma_align_output_res_fp="results/{sample}/kma/{sample}_out_kma.res",
    output:
        ccmetagen_output="results/{sample}/{sample}_ccmetagen/{sample}_ccmetagen.html",
    log:
        "logs/{sample}/ccmetagen.log",
    conda:
        "envs/metagen.yaml"
    shell:
        """
        echo "[INFO] $(date) [ccmetagen] - Running CCMetagen.py on {input.kma_align_output_res_fp}" > {log}
        mkdir -p {output.ccmetagen_output}
        CCMetagen.py -i {input.kma_align_output_res_fp} -o {output.ccmetagen_output}/{wildcards.sample}_ccmetagen >> {log} 2>&1
        """


rule sample_report:
    input:
        ccmetagen_kma_result_fp=rules.run_ccmetagen.output.ccmetagen_output,
        fastqc_report=rules.run_fastqc.output.fastqc_output_dir,
        multiqc_report=rules.run_multiqc.output.multiqc_report_fp,
        kraken_result_fp=rules.run_kraken2.output.kraken_output_fp,
        krona_result_fp=rules.run_krona.output.krona_output_fp,
    output:
        report_fp="results/{sample}/sample_report.md",
    log:
        "logs/{sample}/sample_report.log"
    conda:
        "snakemake"
    script:
        "scripts/generate_report.py"
