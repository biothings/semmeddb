import os
import pandas as pd


def is_valid_object_cui(object_cui: str):
    """
    Currently in semmedVER43_2022_R_PREDICATION.csv there are a few rows with invalid object CUIs, as below:

        (index)      PREDICATION_ID     OBJECT_CUI
        7154043      80264874           1|medd
        7154067      80264901           1|medd
        35698397     109882731          235|Patients
        35700796     109885303          1524|Pain
        48339691     123980473          1|anim
        60007185     137779669          1|dsyn
        69460686     149136787          6|gngm
        80202338     160180312          1|humn
        111403674    192912334          1|neop
        114631930    196519528          1|humn
        114631934    196519532          1|humn

    This function checks the format of the input object CUI.
    """
    # RE is an overkill (w.r.t. semmedVER43_2022_R_PREDICATION.csv); we can simply check if the CUI starts with "C"

    # cui_pattern = re.compile('^[C0-9|.]+$')
    # return cui_pattern.match(object_cui.strip())

    return object_cui.strip().startswith("C")


def construct_documents(row: pd.Series, semantic_type_map):
    """
    SemMedDB Database Details: https://lhncbc.nlm.nih.gov/ii/tools/SemRep_SemMedDB_SKR/dbinfo.html

    Name: PREDICATION table
    Each record in this table identifies a unique predication. The data fields of our interest are as follows:

    PREDICATION_ID  : Auto-generated primary key for each unique predication
    SENTENCE_ID     : Foreign key to the SENTENCE table
    PREDICATE       : The string representation of each predicate (for example TREATS, PROCESS_OF)
    SUBJECT_CUI     : The CUI of the subject of the predication
    SUBJECT_NAME    : The preferred name of the subject of the predication
    SUBJECT_SEMTYPE : The semantic type of the subject of the predication
    SUBJECT_NOVELTY : The novelty of the subject of the predication
    OBJECT_CUI      : The CUI of the object of the predication
    OBJECT_NAME     : The preferred name of the object of the predication
    OBJECT_SEMTYPE  : The semantic type of the object of the predication
    OBJECT_NOVELTY  : The novelty of the object of the predication
    """
    if not is_valid_object_cui(row["OBJECT_CUI"]):
        return

    predication_id = row["PREDICATION_ID"]
    pmid = row["PMID"]
    predicate = row["PREDICATE"]

    sub_cui = row["SUBJECT_CUI"].split("|")
    sub_name = row["SUBJECT_NAME"].split("|")
    sub_semantic_type_abv = row["SUBJECT_SEMTYPE"]
    sub_semantic_type_name = semantic_type_map.get(sub_semantic_type_abv, None)
    sub_novelty = row["SUBJECT_NOVELTY"]

    obj_cui = row["OBJECT_CUI"].split("|")
    obj_name = row["OBJECT_NAME"].split("|")
    obj_semantic_type_abv = row["OBJECT_SEMTYPE"]
    obj_semantic_type_name = semantic_type_map.get(obj_semantic_type_abv, None)
    obj_novelty = row["OBJECT_NOVELTY"]

    sub_id_field = "umls"
    obj_id_field = "umls"

    # Define ID field name
    if "C" not in row["SUBJECT_CUI"]:  # one or more gene ids
        sub_id_field = "ncbigene"
    else:
        if '|' in row["SUBJECT_CUI"]:
            # take first CUI if it contains gene id(s)
            sub_cui = [sub_cui[0]]
            sub_name = [sub_name[0]]

    if "C" not in row["OBJECT_CUI"]:  # one or more gene ids
        obj_id_field = "ncbigene"
    else:
        if '|' in row["OBJECT_CUI"]:  # take first CUI if it contains gene id(s)
            obj_cui = [obj_cui[0]]
            obj_name = [obj_name[0]]

    id_count = 0  # loop to get all id combinations if one record has multiple ids
    for sub_idx, sub_id in enumerate(sub_cui):
        for obj_idx, obj_id in enumerate(obj_cui):

            id_count += 1
            if len(sub_cui) == 1 and len(obj_cui) == 1:
                _id = predication_id
            else:
                _id = predication_id + "_" + str(id_count)  # add sequence id

            doc = {
                "_id": _id,
                "predicate": predicate,
                "predication_id": predication_id,
                "pmid": pmid,
                "subject": {
                    sub_id_field: sub_id,
                    "name": sub_name[sub_idx],
                    "semantic_type_abbreviation": sub_semantic_type_abv,
                    "semantic_type_name": sub_semantic_type_name,
                    "novelty": sub_novelty
                },
                "object": {
                    obj_id_field: obj_id,
                    "name": obj_name[obj_idx],
                    "semantic_type_abbreviation": obj_semantic_type_abv,
                    "semantic_type_name": obj_semantic_type_name,
                    "novelty": obj_novelty
                }
            }

            # del semtype_name field if we did not any mappings
            if not sub_semantic_type_name:
                del doc["subject"]["semantic_type_name"]
            if not obj_semantic_type_name:
                del doc["object"]["semantic_type_name"]
            yield doc


def read_semantic_type_data_frame(data_folder, filename) -> pd.DataFrame:
    filepath = os.path.join(data_folder, filename)
    column_info = [
        (0, 'abv', str),
        # (1, 'ID', str),
        (2, 'label', str)
    ]
    column_indices = [e[0] for e in column_info]
    column_names = [e[1] for e in column_info]
    column_dtypes = {e[1]: e[2] for e in column_info}
    data_frame = pd.read_csv(filepath, sep="|", names=column_names, usecols=column_indices, dtype=column_dtypes)
    return data_frame


def read_semmed_data_frame(data_folder, filename) -> pd.DataFrame:
    filepath = os.path.join(data_folder, filename)
    encoding = "latin1"  # TODO encode in UTF-8 before outputting
    na_value = r"\N"
    column_info = [
        # Each element is a tuple of (column_index, column_name, data_type)
        #   See column description at https://lhncbc.nlm.nih.gov/ii/tools/SemRep_SemMedDB_SKR/dbinfo.html
        # "Int8" is a nullable integer type (while `int` cannot handle NA values), range [-128, 127]
        # "UInt32" ranges [0, 4294967295]
        #   See https://pandas.pydata.org/docs/user_guide/basics.html#basics-dtypes
        (0, "PREDICATION_ID", str),      # column 0 (Auto-generated primary key; read as strings for easier concatenation)
        # (1, "SENTENCE_ID", str),         # column 1 (ignored)
        (2, "PMID", "UInt32"),           # column 2 (PubMed IDs are 8-digit numbers)
        (3, "PREDICATE", str),           # column 3
        (4, "SUBJECT_CUI", str),         # column 4
        (5, "SUBJECT_NAME", str),        # column 5
        (6, "SUBJECT_SEMTYPE", str),     # column 6
        (7, "SUBJECT_NOVELTY", "Int8"),  # column 7
        (8, "OBJECT_CUI", str),          # column 8
        (9, "OBJECT_NAME", str),         # column 9
        (10, "OBJECT_SEMTYPE", str),     # column 10
        (11, "OBJECT_NOVELTY", "Int8")   # column 11
        # (12, "FACT_VALUE", "Int8"),      # column 12 (ignored)
        # (13, "MOD_SCALE", "Int8"),       # column 13 (ignored)
        # (14, "MOD_VALUE", "Int8"),       # column 14 (ignored)
    ]
    column_indices = [e[0] for e in column_info]
    column_names = [e[1] for e in column_info]
    column_dtypes = {e[1]: e[2] for e in column_info}
    data_frame = pd.read_csv(filepath, names=column_names, sep=",", usecols=column_indices,
                             dtype=column_dtypes, na_values=[na_value], encoding=encoding)
    return data_frame


def load_data(data_folder):
    semantic_type_df = read_semantic_type_data_frame(data_folder, "SemanticTypes_2013AA.txt")
    semantic_type_map = dict(zip(semantic_type_df["abv"], semantic_type_df["label"]))

    semmed_df = read_semmed_data_frame(data_folder, "semmedVER43_2022_R_PREDICATION.csv")
    for _, row in semmed_df.iterrows():
        yield from construct_documents(row, semantic_type_map)

