import io
import os
import pydoc
import socket
import polars as pl
from argparse import ArgumentParser
from mohd_manifest_cli.constants import *

def parser_args():
    parser = ArgumentParser(description="Create a manifest for a given molecular data type. Pulls from the refrerence Google Sheet.")
    parser.add_argument('molecular_data_type', type=str, choices=['RNA', 'ATAC', 'WGS', 'WGBS'])
    parser.add_argument('--range', type=int, nargs=2, metavar=('START', 'END'), default=(1, -1), help="Range defining which MOHD Accessions to process. Half-closed interval.")
    parser.add_argument('--dryrun', action="store_true", help="Do not write the metadata.tsv file")
    args = parser.parse_args()
    return args

def main():
    args = parser_args()
    expression = args.range[0] < args.range[1] 
    if args.range[1] == -1:
        expression = True
    assert expression, f"Invalid Range: {args.range}"

    valid_aliases = ['z010', 'z011', 'z012', 'z013', 'z014', 'z020', 'z021', 'z022']
    hostname_fmt_str = "{alias}.internal.wenglab.org"
    valid_hostnames = [hostname_fmt_str.format(alias=alias) for alias in valid_aliases]
    assert socket.gethostname() in valid_hostnames, f"This CLI must be executed on one of the ZLab servers. Valid hostnames are {valid_hostnames}"

    path = MAPPING_FILE_GLOB_EXP_FORMAT_STRING.format(mol=args.molecular_data_type)
    print(path)
    molecular_df = pl.read_csv(path, separator="\t", schema=pl.Schema({'opc_id': pl.Utf8, 'mohd_accession': pl.Utf8}))

    buffer = io.StringIO()
    molecular_df.write_csv(buffer, separator='\t')
    pydoc.pager(buffer.getvalue()) # inspect output
    
    num_range = range(args.range[0], args.range[1]) # User is expected to provide a half-closed interval
    molecular_df = molecular_df.with_columns(pl.col('mohd_accession').str.strip_prefix(MOLECULAR_PREFIX_MAP[args.molecular_data_type]).cast(pl.UInt32).alias('numerical_part'))

    if args.range[1] == -1: # -1 means "to the end"
        max_numerical_part = int(molecular_df['numerical_part'].max()) # type: ignore
        num_range = range(args.range[0], max_numerical_part + 1)

    molecular_df = molecular_df.filter(pl.col('numerical_part').is_in(num_range))
    numerical_parts = molecular_df['numerical_part'].to_list()
    opc_ids = molecular_df['opc_id'].to_list()

    molecular_subdir = MOLECULAR_SUBDIR_MAP[args.molecular_data_type]
    fastq_records= []
    for opc_id, numerical_part in zip(opc_ids, numerical_parts):
        fq_f = FORMAT_STRING_MAP[args.molecular_data_type].format(numerical_part)
        r1_path = os.path.join(SRC, molecular_subdir, fq_f, fq_f + "_R1.fastq.gz")
        r2_path = os.path.join(SRC, molecular_subdir, fq_f, fq_f + "_R2.fastq.gz")

        assert os.path.exists(r1_path), f"File {r1_path} does not exist"
        assert os.path.exists(r2_path), f"File {r2_path} does not exist"

        fastq_records.append({
            'Dataset': opc_id,
            'Barcode': FORMAT_STRING_MAP[args.molecular_data_type].format(numerical_part).lstrip("MOHD_"),
            'File1': r1_path,
            'File2': r2_path,
        })

    fastq_df = pl.from_records(fastq_records)

    if not args.molecular_data_type == 'WGBS':
        fastq_df = fastq_df.drop('Dataset').rename({'Barcode': 'Sample', 'File1': 'R1', 'File2': 'R2'})

    buffer = io.StringIO()
    fastq_df.write_csv(buffer, separator='\t')
    pydoc.pager(buffer.getvalue()) # inspect output

    fastq_df.write_csv("metadata.tsv", separator='\t')

if __name__ == "__main__":
    main()
