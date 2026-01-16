GOOGLE_SHEET_NAME = "MOHD DATA PROCESSING"
CREDENTIALS_FILE = "credentials.json"
FORMAT_STRING_MAP = {
    'RNA': "MOHD_ER1{:05d}",
    'ATAC': "MOHD_EA1{:05d}",
    'WGS': "MOHD_EG1{:05d}",
    'WGBS': "MOHD_EB1{:05d}"
}
MOLECULAR_SUBDIR_MAP = {
    'RNA': "3_RNA",
    'ATAC': "2_ATAC",
    'WGS': "0_WGS",
    'WGBS': "1_WGBS"
}
MOLECULAR_PREFIX_MAP = {
    'RNA': "MOHD_ER1",
    'ATAC': "MOHD_EA1",
    'WGS': "MOHD_EG1",
    'WGBS': "MOHD_EB1"
}
SRC = "/data/projects/mohd/data/Molecular"



