from snakemake.script import snakemake
from pathlib import Path
import markdown

def write_report(sample:str, report_fp:str, ccmetagen_fp:str):
    with open(report_fp, "w") as md_fout:
        md_fout.write(f"# 🧪 FLARE Report on Sample {sample}\n")
        md_fout.write(f"- [CCMetagen krona report]({ccmetagen_fp.resolve()})\n")

def markdown_to_html(sample:str, markdown_fp:str, html_fp:str):
    md_content=open(markdown_fp, "r").read()
    html_body = markdown.markdown(
        md_content,
        extension=["fenced_code", "tables", "codehilite"]
    )
    with open(html_fp,"w") as html_fout:
        html_fout.write(
f"""<!DOCTYPE html>
<html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>FLARE - {sample}</title>
        <style>
            body {{ font-family: sans-serif; margin: 40px;}}
            pre code {{ background-color: #f4f4f4; padding: 10px; display: block; overflow-x: auto; }}
            h1, h2, h3 {{ color: #333; }} 
        </style>
    </head>
    <body>
    {html_body}
    </body>
</html>
""")
        
write_report(snakemake.wildcards.sample,
             Path(snakemake.output["report_fp"]),
             Path(snakemake.input["ccmetagen_kma_result_fp"]),
             )

markdown_to_html(snakemake.wildcards.sample,
                 Path(snakemake.output["report_fp"]),
                 Path(snakemake.output["report_fp"].replace(".md",".html"))
                 )