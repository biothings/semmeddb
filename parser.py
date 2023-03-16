import os
from pathlib import Path
import pickle
import requests
import pandas as pd
import numpy as np

from typing import Dict, Set
from collections.abc import Collection  # for type hints

from biothings.utils.common import iter_n

"""
Constants of filenames and directories
"""
UMLS_METATHESAURUS_DIR = "2022AB/META"
MRCUI_FN = "MRCUI.RRF"

SEMTYPE_MAPPING_FN = "SemanticTypes_2018AB.txt"
UMLS_PREFERRED_CUI_NAME_SEMTYPE_FN = "UMLS_CUI_Semtype_2022AB.tsv"

SEMMED_PREDICATION_FN = "semmedVER43_2023_R_PREDICATION.116080.csv"
SEMMED_SENTENCE_FN = "semmedVER43_2023_R_SENTENCE.116080.csv"

CACHE_DIR = "CACHE"
_SEMMED_PREDICATION_PATH = Path(SEMMED_PREDICATION_FN)
# Path("semmedVER43_2023_R_PREDICATION.116080_clean_pyarrow_snappy.parquet")
SEMMED_PREDICATION_CACHE_FN = _SEMMED_PREDICATION_PATH.with_stem(_SEMMED_PREDICATION_PATH.stem + "_clean_pyarrow_snappy").with_suffix(".parquet")
# Path("semmedVER43_2023_R_PREDICATION.116080_NodeNorm.pickle")
SEMMED_NODE_NORM_RESPONSE_CACHE_FN = _SEMMED_PREDICATION_PATH.with_stem(_SEMMED_PREDICATION_PATH.stem + "_NodeNorm").with_suffix(".pickle")


###################################
# PART 1: Load Semantic Type Data #
###################################

def read_semantic_type_mappings_data_frame(filepath) -> pd.DataFrame:
    separator = "|"
    column_info = [
        # See column description at https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/SemanticTypesAndGroups.html
        (0, 'abbreviation', "string"),
        # (1, 'TUI', "string"),
        (2, 'fullname', "string")
    ]
    column_indices = [e[0] for e in column_info]
    column_names = [e[1] for e in column_info]
    column_dtypes = {e[1]: e[2] for e in column_info}
    data_frame = pd.read_csv(filepath, sep=separator, names=column_names, usecols=column_indices, dtype=column_dtypes)

    data_frame = data_frame.astype({
        "abbreviation": "string[pyarrow]",
        "fullname": "string[pyarrow]"
    })

    return data_frame


def get_semtype_name_map(semantic_type_data_frame: pd.DataFrame) -> Dict:
    """
    Get a map of <abbreviation, fullname> of Semantic Types
    """
    return dict(zip(semantic_type_data_frame["abbreviation"], semantic_type_data_frame["fullname"]))


#################################
# PART 2: Load Retired CUI Data #
#################################

def read_mrcui_data_frame(filepath):
    separator = "|"
    column_info = [
        # Each element is a tuple of (column_index, column_name, data_type)
        #   See column description at https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.retired_cui_mapping_file_mrcui_rr/
        (0, "CUI1", "string"),  # column 0
        # (1, "VER", "string"),  # column 1 (ignored)
        (2, "REL", "category"),  # column 2
        # (3, "RELA", "string"),  # column 3 (ignored)
        # (4, "MAPREASON", "string"),  # column 4 (ignored)
        (5, "CUI2", "string"),  # column 5
        # (6, "MAPIN", "string")  # column 6 (ignored). We confirmed that CUI1 and CUI2 columns has no CUIs in common
    ]
    column_indices = [e[0] for e in column_info]
    column_names = [e[1] for e in column_info]
    column_dtypes = {e[1]: e[2] for e in column_info}
    data_frame = pd.read_csv(filepath, sep=separator, names=column_names, usecols=column_indices, dtype=column_dtypes)

    data_frame = data_frame.astype({
        "CUI1": "string[pyarrow]",
        "CUI2": "string[pyarrow]"
    })

    return data_frame


def get_retired_cuis_for_deletion(mrcui_data_frame: pd.DataFrame) -> Set:
    deletion_flags = (mrcui_data_frame["REL"] == "DEL")
    deleted_cuis = mrcui_data_frame.loc[deletion_flags, "CUI1"].unique()
    return set(deleted_cuis)


def get_retirement_mapping_data_frame(mrcui_data_frame: pd.DataFrame) -> pd.DataFrame:
    # Exclude rows whose "CUI2" is empty
    mapping_data_frame = mrcui_data_frame.loc[~mrcui_data_frame["CUI2"].isnull(), ["CUI1", "CUI2"]]
    return mapping_data_frame


def add_cui_name_and_semtype_to_retirement_mapping(retirement_mapping_data_frame, semmed_cui_name_semtype_data_frame, umls_cui_name_semtype_data_frame):
    """
    Given a replacement CUI (for a retired CUI), its name and semtype should be looked up in the SemMedDB data frame at first.
    If not present, look up in the external "UMLS_CUI_Semtype.tsv" file.

    This function will match the CUI names semtypes to the replacement CUIs (as in the "CUI2" column) of the "retirement_mapping_data_frame". CUI names and
    semtypes from SemMedDB will be preferred for matching.
    """

    new_cuis = set(retirement_mapping_data_frame["CUI2"].unique())
    semmed_cui_info = semmed_cui_name_semtype_data_frame.loc[semmed_cui_name_semtype_data_frame["CUI"].isin(new_cuis)]
    umls_cui_info = umls_cui_name_semtype_data_frame.loc[umls_cui_name_semtype_data_frame["CUI"].isin(new_cuis)]
    preferred_cui_info = pd.concat([semmed_cui_info, umls_cui_info], ignore_index=True, copy=False)
    # because SemMed values are put above UMLS values in "preferred_cui_info", so keep="first" will preserve the SemMed values if duplicates are found
    preferred_cui_info.drop_duplicates(subset=["CUI", "SEMTYPE"], keep="first", inplace=True)

    """
    Why left join here? Because "retirement_mapping_df" ("MRCUI.RRF") may contain a replacement CUI having no preferred English name and thus not
        included in "umls_cui_name_semtype_data_frame" ("UMLS_CUI_Semtype.tsv")
    E.g. C4082455 is replaced by C4300557, according to "MRCUI.RRF". However C4300557 has only one preferred name in French, "Non-disjonction mitotique"
    Left join would cause NaN values (which means a failed replacement) for C4082455. Such retired CUIs will be deleted directly. Therefore, for now we should 
        keep C4082455 in the result.
    """
    retirement_mapping_data_frame = retirement_mapping_data_frame.merge(preferred_cui_info, how="left", left_on="CUI2", right_on="CUI")
    retirement_mapping_data_frame.drop(columns="CUI", inplace=True)
    retirement_mapping_data_frame.rename(columns={"CONCEPT_NAME": "CUI2_NAME", "SEMTYPE": "CUI2_SEMTYPE"}, inplace=True)

    return retirement_mapping_data_frame


#########################################################
# PART 3: Load CUI Names/Semantic Types for Replacement #
#########################################################


def read_cui_name_and_semtype_from_umls(filepath) -> pd.DataFrame:
    separator = "\t"
    column_info = [
        # Each element is a tuple of (column_index, column_name, data_type)
        (0, "CUI", "string"),
        (1, "CONCEPT_NAME", "string"),
        # we will map semantic type abbreviations to fullnames when constructing documents later, no need to read this column for now
        # (2, "SEMTYPE_FULLNAME", "string"),
        (3, "SEMTYPE", "string")
    ]
    column_indices = [e[0] for e in column_info]
    column_names = [e[1] for e in column_info]
    column_dtypes = {e[1]: e[2] for e in column_info}
    # Ignore the original header, use column names defined above
    data_frame = pd.read_csv(filepath, sep=separator, header=0, names=column_names, usecols=column_indices, dtype=column_dtypes)

    data_frame = data_frame.astype({
        "CUI": "string[pyarrow]",
        "CONCEPT_NAME": "string[pyarrow]",
        "SEMTYPE": "string[pyarrow]"
    })

    return data_frame


############################
# PART 4: Load SemMed Data #
############################

def read_semmed_sentence_data_frame(filepath) -> pd.DataFrame:
    separator = ","
    escapechar = "\\"  # single backslash, see https://github.com/biothings/semmeddb/issues/10
    column_info = [
        # See column description of the SENTENCE table at https://lhncbc.nlm.nih.gov/ii/tools/SemRep_SemMedDB_SKR/dbinfo.html
        # Note that column order in CSV is different from the SQL table.
        (0, "SENTENCE_ID", "UInt32"),  # Auto-generated primary key for each sentence
        # (1, "PMID", "UInt32"),
        # (2, "TYPE", "string"),  # 'ti' for the title of the citation, 'ab' for the abstract
        # (3, "NUMBER", "string"),  # The location of the sentence within the title or abstract
        # (4, "SENT_START_INDEX", "UInt32"),  # The character position within the text of the MEDLINE citation of the first character of the sentence
        (5, "SENTENCE", "string"),  # The actual string or text of the sentence
        # (6, "SENT_END_INDEX", "UInt32"),  # The character position within the text of the MEDLINE citation of the last character of the sentence
        # (7, "SECTION_HEADER", "string"),  # Section header name of structured abstract
        # (8, "NORMALIZED_SECTION_HEADER", "string"),  # Normalized section header name
    ]

    column_indices = [e[0] for e in column_info]
    column_names = [e[1] for e in column_info]
    column_dtypes = {e[1]: e[2] for e in column_info}
    data_frame = pd.read_csv(filepath, sep=separator, names=column_names, usecols=column_indices, dtype=column_dtypes, escapechar=escapechar)

    data_frame = data_frame.astype({
        "SENTENCE": "string[pyarrow]"
    })

    return data_frame


def get_semmed_sentence_map(semmed_sentence_data_frame: pd.DataFrame, semmed_predication_data_frame: pd.DataFrame) -> set:
    """
    Subset the sentence data frame by unique sentence IDs in the predication data frame, and transform the sentence two-column data frame into a dictionary of
    <SENTENCE_ID: int, SENTENCE: string>.
    """
    # Drop sentence rows whose SENTENCE_ID is not present in the predication data frame
    sentence_ids = set(semmed_predication_data_frame["SENTENCE_ID"].unique())
    presence_flags = semmed_sentence_data_frame["SENTENCE_ID"].isin(sentence_ids)
    absence_index = semmed_sentence_data_frame.index[~presence_flags]
    semmed_sentence_data_frame.drop(index=absence_index, inplace=True)  # drop in-place to save memory

    semmed_sentence_mapping = semmed_sentence_data_frame.set_index("SENTENCE_ID").to_dict()["SENTENCE"]
    return semmed_sentence_mapping


def read_semmed_predication_data_frame(filepath) -> pd.DataFrame:
    encoding = "latin1"  # file may contain chars in other languages (e.g. French)
    separator = ","
    na_value = r"\N"
    escapechar = "\\"  # single backslash, see https://github.com/biothings/semmeddb/issues/10
    column_info = [
        # Each element is a tuple of (column_index, column_name, data_type)
        #   See column description at https://lhncbc.nlm.nih.gov/ii/tools/SemRep_SemMedDB_SKR/dbinfo.html
        # "Int8" is a nullable integer type (while `int` cannot handle NA values), range [-128, 127]
        # "UInt32" ranges [0, 4294967295]
        #   See https://pandas.pydata.org/docs/user_guide/basics.html#basics-dtypes
        (0, "PREDICATION_ID", "UInt32"),  # column 0 (Auto-generated primary key; current max is 199,713,830)
        (1, "SENTENCE_ID", "UInt32"),  # column 1 (Auto-generated foreign key; current max is 395,464,361)
        (2, "PMID", "UInt32"),  # column 2 (PubMed IDs are 8-digit numbers)
        (3, "PREDICATE", "string"),  # column 3
        (4, "SUBJECT_CUI", "string"),  # column 4
        (5, "SUBJECT_NAME", "string"),  # column 5
        (6, "SUBJECT_SEMTYPE", "string"),  # column 6
        (7, "SUBJECT_NOVELTY", "Int8"),  # column 7 (Currently either 0 or 1)
        (8, "OBJECT_CUI", "string"),  # column 8
        (9, "OBJECT_NAME", "string"),  # column 9
        (10, "OBJECT_SEMTYPE", "string"),  # column 10
        (11, "OBJECT_NOVELTY", "Int8")  # column 11 (Currently either 0 or 1)
        # (12, "FACT_VALUE", "Int8"),  # column 12 (ignored)
        # (13, "MOD_SCALE", "Int8"),  # column 13 (ignored)
        # (14, "MOD_VALUE", "Int8"),  # column 14 (ignored)
    ]
    column_indices = [e[0] for e in column_info]
    column_names = [e[1] for e in column_info]
    column_dtypes = {e[1]: e[2] for e in column_info}
    data_frame = pd.read_csv(filepath, sep=separator, names=column_names, usecols=column_indices,
                             dtype=column_dtypes, na_values=[na_value], encoding=encoding, escapechar=escapechar)

    data_frame = data_frame.astype({
        "PREDICATE": "string[pyarrow]",
        "SUBJECT_CUI": "string[pyarrow]",
        "SUBJECT_NAME": "string[pyarrow]",
        "SUBJECT_SEMTYPE": "string[pyarrow]",
        "OBJECT_CUI": "string[pyarrow]",
        "OBJECT_NAME": "string[pyarrow]",
        "OBJECT_SEMTYPE": "string[pyarrow]"
    })

    return data_frame


def delete_invalid_object_cuis(predication_data_frame: pd.DataFrame):
    """
    This function remove rows with "invalid" object CUIs in the Semmed data frame.
    ote this operation must be done BEFORE "explode_pipes()" is called.

    A "valid" object CUI present in "semmedVER43_2022_R_PREDICATION.csv" can be either:
        1. A true CUI (starting with "C", followed by seven numbers, like "C0003725")
        2. A NCBI gene ID (a numerical string, like "1756")
        3. A piped string of a true CUI and multiple NCBI gene IDs (like "C1414968|2597")
        4. A piped string of multiple NCBI gene IDs (like "4914|7170")

    Currently in "semmedVER43_2022_R_PREDICATION.csv" there are a few rows with invalid object CUIs, as below:

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

    Subject CUIs are all valid in "semmedVER43_2022_R_PREDICATION.csv".
    """

    """
    Below is the previous row-wise filter. Issues:
    1. Object CUIs don't contain spaces; `strip()` operation unnecessary
    2. Object CUIs don't contain dots; RE pattern can be simplified
    3. Row-wise RE matching is slow; `pd.Series.str.match()` is much faster
    """
    # cui_pattern = re.compile(r'^[C0-9|.]+$')  # multiple occurrences of "C", "0" to "9", "|" (vertical bar), or "." (dot)
    # return cui_pattern.match(object_cui.strip())

    cui_pattern = r"^[C0-9|]+$"  # multiple occurrences of "C", "0" to "9", or "|" (vertical bar)
    valid_flags = predication_data_frame["OBJECT_CUI"].str.match(cui_pattern)
    invalid_index = predication_data_frame.index[~valid_flags]
    predication_data_frame.drop(index=invalid_index, inplace=True)
    predication_data_frame.reset_index(drop=True, inplace=True)
    return predication_data_frame


def delete_zero_novelty_scores(predication_data_frame: pd.DataFrame):
    """
    Rows with novelty score equal to 0 should be removed.
    See discussion in https://github.com/biothings/pending.api/issues/63#issuecomment-1100469563
    """
    zero_novelty_flags = predication_data_frame["SUBJECT_NOVELTY"].eq(0) | predication_data_frame["OBJECT_NOVELTY"].eq(0)
    zero_novelty_index = predication_data_frame.index[zero_novelty_flags]
    predication_data_frame.drop(index=zero_novelty_index, inplace=True)
    predication_data_frame.reset_index(drop=True, inplace=True)
    return predication_data_frame


def explode_pipes(predication_data_frame: pd.DataFrame):
    """
    Split "SUBJECT_CUI", "SUBJECT_NAME", "OBJECT_CUI", and "OBJECT_NAME" by pipes. Then transform the split values into individual rows.

    E.g. given the original data

        PREDICATION_ID  SUBJECT_CUI     SUBJECT_NAME          OBJECT_CUI    OBJECT_NAME
        11021926        2212|2213|9103  FCGR2A|FCGR2B|FCGR2C  C1332714|920  CD4 gene|CD4

    After splitting by pipes, we have

        PREDICATION_ID  SUBJECT_CUI       SUBJECT_NAME            OBJECT_CUI      OBJECT_NAME
        11021926        [2212,2213,9103]  [FCGR2A,FCGR2B,FCGR2C]  [C1332714,920]  [CD4 gene,CD4]

    After the "explode" operations (see https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.explode.html), we have

        PREDICATION_ID  SUBJECT_CUI  SUBJECT_NAME  OBJECT_CUI  OBJECT_NAME
        11021926        2212         FCGR2A        C1332714    CD4 gene
        11021926        2213         FCGR2B        C1332714    CD4 gene
        11021926        9103         FCGR2C        C1332714    CD4 gene
        11021926        2212         FCGR2A        920         CD4
        11021926        2213         FCGR2B        920         CD4
        11021926        9103         FCGR2C        920         CD4
    """

    sub_piped_flags = predication_data_frame["SUBJECT_CUI"].str.contains(r"\|")
    obj_piped_flags = predication_data_frame["OBJECT_CUI"].str.contains(r"\|")
    # These two indices are necessary to locate equivalent NCBIGene IDs
    predication_data_frame["IS_SUBJECT_PIPED"] = sub_piped_flags
    predication_data_frame["IS_OBJECT_PIPED"] = obj_piped_flags

    piped_flags = sub_piped_flags | obj_piped_flags
    predication_data_frame["IS_PIPED"] = piped_flags
    predication_data_frame.set_index("IS_PIPED", append=False, inplace=True)  # use "IS_PIPED" as the new index; discard the original integer index

    piped_predications = predication_data_frame.loc[True]

    piped_predications = piped_predications.assign(
        OBJECT_CUI=piped_predications["OBJECT_CUI"].str.split(r"\|"),
        OBJECT_NAME=piped_predications["OBJECT_NAME"].str.split(r"\|"),
        SUBJECT_CUI=piped_predications["SUBJECT_CUI"].str.split(r"\|"),
        SUBJECT_NAME=piped_predications["SUBJECT_NAME"].str.split(r"\|")
    )

    piped_predications = piped_predications.explode(["OBJECT_CUI", "OBJECT_NAME"])
    piped_predications = piped_predications.explode(["SUBJECT_CUI", "SUBJECT_NAME"])
    # These 4 columns' dtypes are changed to "object" after the above "assign" and "explode" operations
    # Convert them to "string[pyarrow]" for less memory usage
    piped_predications = piped_predications.astype({
        "SUBJECT_CUI": "string[pyarrow]",
        "SUBJECT_NAME": "string[pyarrow]",
        "OBJECT_CUI": "string[pyarrow]",
        "OBJECT_NAME": "string[pyarrow]",
    })

    """
    "CUI" columns may contain empty strings and "NAME" columns may contain "None" strings, e.g.:
    
        PREDICATION_ID  SUBJECT_CUI          SUBJECT_NAME              OBJECT_CUI           OBJECT_NAME
        72530597        C0757738||100329167  m-AAA protease|None|AAA1  C1330957             Cytokinesis of the fertilized ovum
        75458336        C1167321             inner membrane            C0757738||100329167  m-AAA protease|None|AAA1
        
    Rows containing such values after "explode" operations should be dropped.
    """
    piped_predications.reset_index(drop=False, inplace=True)  # switch to the integer index, for ".drop(index=?)" operation below
    empty_value_flags = \
        piped_predications["SUBJECT_CUI"].eq('') | piped_predications["SUBJECT_NAME"].eq('None') | \
        piped_predications["OBJECT_CUI"].eq('') | piped_predications["OBJECT_NAME"].eq('None')
    empty_value_index = piped_predications.index[empty_value_flags]
    piped_predications.drop(index=empty_value_index, inplace=True)
    piped_predications.set_index("IS_PIPED", append=False, inplace=True)  # switch back to the "IS_PIPED" index, for ".concat()" operation below

    predication_data_frame.drop(index=True, inplace=True)  # drop the original piped predications (marked by True values in "IS_PIPED" index)
    predication_data_frame = pd.concat([predication_data_frame, piped_predications], copy=False)  # append the "exploded" piped predications
    predication_data_frame.reset_index(drop=True, inplace=True)  # drop the "IS_PIPED" index (no longer needed)

    return predication_data_frame


def delete_retired_cuis(predication_data_frame: pd.DataFrame, retired_cuis: Set):
    """
    Remove rows containing deleted CUIs specified in "MRCUI.RRF" file.
    Note this operation must be done AFTER "explode_pipes()" is called.
    """
    deleted_flags = predication_data_frame["OBJECT_CUI"].isin(retired_cuis) | predication_data_frame["SUBJECT_CUI"].isin(retired_cuis)
    deleted_index = predication_data_frame.index[deleted_flags]
    predication_data_frame.drop(index=deleted_index, inplace=True)
    predication_data_frame.reset_index(drop=True, inplace=True)
    return predication_data_frame


def add_prefix_columns(predication_data_frame: pd.DataFrame):
    """
    Add 2 columns, "SUBJECT_PREFIX" and "OBJECT_PREFIX" to the SemMedDB data frame.
    If a "CUI" is a real CUI starting with the letter "C", its prefix would be "umls";
    otherwise the "CUI" should be a NCBIGene ID, and its prefix would be "ncbigene".
    """
    subject_prefix_series = pd.Series(np.where(predication_data_frame["SUBJECT_CUI"].str.startswith("C"), "umls", "ncbigene"), dtype="category")
    object_prefix_series = pd.Series(np.where(predication_data_frame["OBJECT_CUI"].str.startswith("C"), "umls", "ncbigene"), dtype="category")

    predication_data_frame = predication_data_frame.assign(
        SUBJECT_PREFIX=subject_prefix_series,
        OBJECT_PREFIX=object_prefix_series
    )

    return predication_data_frame


def get_cui_name_and_semtype_from_semmed(predication_data_frame: pd.DataFrame):
    sub_cui_flags = predication_data_frame["SUBJECT_PREFIX"].eq("umls")
    obj_cui_flags = predication_data_frame["OBJECT_PREFIX"].eq("umls")

    sub_cui_semtype_data_frame = predication_data_frame.loc[sub_cui_flags, ["SUBJECT_CUI", "SUBJECT_NAME", "SUBJECT_SEMTYPE"]]
    obj_cui_semtype_data_frame = predication_data_frame.loc[obj_cui_flags, ["OBJECT_CUI", "OBJECT_NAME", "OBJECT_SEMTYPE"]]

    """
    Drop duplicates in advance in order to:
    1. reduce memory usage, and
    2. avoid the "ArrowInvalid: offset overflow while concatenating arrays" error due to a bug in Apache Arrow.
    
    See https://issues.apache.org/jira/browse/ARROW-10799 for the bug details
    """
    sub_cui_semtype_data_frame.drop_duplicates(subset=["SUBJECT_CUI", "SUBJECT_SEMTYPE"], inplace=True)
    obj_cui_semtype_data_frame.drop_duplicates(subset=["OBJECT_CUI", "OBJECT_SEMTYPE"], inplace=True)

    unified_column_names = ["CUI", "CONCEPT_NAME", "SEMTYPE"]
    sub_cui_semtype_data_frame.columns = unified_column_names
    obj_cui_semtype_data_frame.columns = unified_column_names

    cui_semtype_data_frame = pd.concat([sub_cui_semtype_data_frame, obj_cui_semtype_data_frame], ignore_index=True, copy=False)
    cui_semtype_data_frame.drop_duplicates(subset=["CUI", "SEMTYPE"], inplace=True)
    return cui_semtype_data_frame


def map_retired_cuis(predication_data_frame: pd.DataFrame, retirement_mapping_data_frame: pd.DataFrame):
    """
    Let's rename:

    - predication_data_frame as table A(SUBJECT_CUI, SUBJECT_NAME, SUBJECT_SEMTYPE, OBJECT_CUI, OBJECT_NAME, OBJECT_SEMTYPE), the target of replacement,
    - retirement_mapping_data_frame as table B(CUI1, CUI2, CUI2_NAME, CUI2_SEMTYPE) where CUI1 is the retired CUI column while CUI2 is new CUI column, and

    The replacement is carried out in the following steps:

    1. Find from A all predications with retired subjects or objects, resulting in table X
    2. Replace predications with retired subjects
        2.1 X.merge(B, how="left", left_on=[SUBJECT_CUI, SUBJECT_SEMTYPE], right_on=[CUI1, CUI2_SEMTYPE]), i.e. find the same semantic-typed new CUI2
            for each retired SUBJECT_CUI, resulting in table Y
        2.2 Replace columns (SUBJECT_CUI, SUBJECT_NAME, SUBJECT_SEMTYPE) in Y with matched (CUI2, CUI2_NAME, CUI2_SEMTYPE), resulting in table Z
    3. Replace predications with retired objects
        3.1 Z.merge(B, how="left", left_on=[SUBJECT_CUI, SUBJECT_SEMTYPE], right_on=[CUI1, CUI2_SEMTYPE]), i.e. find the same semantic-typed new CUI2
            for each retired SUBJECT_CUI, resulting in table W
        3.2 Replace columns (SUBJECT_CUI, SUBJECT_NAME, SUBJECT_SEMTYPE) in W with matched (CUI2, CUI2_NAME, CUI2_SEMTYPE), resulting in table V
    4. Drop X from A, and then append V to A, resulting in table U. Return U as result.
    """

    ##########
    # Step 1 #
    ##########
    """
    Find all retired CUIs to be replaced.

    P.S. Do not use set(mapping_data_frame["CUI1"].unique()). See comments below.
    E.g. CUI C4082455 should be replaced to C4300557. However C4300557 is not in the "mapping_data_frame"
    In this case, all predications with CUI C4082455 should also be marked by "replaced_sub_flags" and be deleted later
    """
    retired_cuis = set(retirement_mapping_data_frame["CUI1"].unique())
    sub_retired_flags = predication_data_frame["SUBJECT_CUI"].isin(retired_cuis)
    obj_retired_flags = predication_data_frame["OBJECT_CUI"].isin(retired_cuis)
    retired_flags = sub_retired_flags | obj_retired_flags

    predication_data_frame["IS_SUBJECT_RETIRED"] = sub_retired_flags
    predication_data_frame["IS_OBJECT_RETIRED"] = obj_retired_flags

    # It does not matter if "retired_predications" is a view or a copy of "predication_data_frame"
    #   since the below "merge" operation always returns a new dataframe.
    # Therefore, operations on "retired_predications" won't alter "predication_data_frame".
    retired_predications = predication_data_frame.loc[retired_flags]

    ##########
    # Step 2 #
    ##########
    retired_predications = retired_predications.merge(retirement_mapping_data_frame, how="left",
                                                      left_on=["SUBJECT_CUI", "SUBJECT_SEMTYPE"],
                                                      right_on=["CUI1", "CUI2_SEMTYPE"])  # match by CUIs and semtypes together
    # Overwrite retired SUBJECT_* values with matched CUI2_* values
    retired_predications["SUBJECT_CUI"] = np.where(retired_predications["IS_SUBJECT_RETIRED"],
                                                   retired_predications["CUI2"], retired_predications["SUBJECT_CUI"])
    retired_predications["SUBJECT_NAME"] = np.where(retired_predications["IS_SUBJECT_RETIRED"],
                                                    retired_predications["CUI2_NAME"], retired_predications["SUBJECT_NAME"])
    retired_predications["SUBJECT_SEMTYPE"] = np.where(retired_predications["IS_SUBJECT_RETIRED"],
                                                       retired_predications["CUI2_SEMTYPE"], retired_predications["SUBJECT_SEMTYPE"])

    # Drop all merged columns from retirement_mapping_data_frame
    retired_predications.drop(columns=retirement_mapping_data_frame.columns, inplace=True)
    # Drop all predications whose retired subjects are unmatched
    retired_predications.dropna(axis=0, how="any", subset=["SUBJECT_CUI", "SUBJECT_NAME", "SUBJECT_SEMTYPE"], inplace=True)

    ##########
    # Step 3 #
    ##########
    retired_predications = retired_predications.merge(retirement_mapping_data_frame, how="left",
                                                      left_on=["OBJECT_CUI", "OBJECT_SEMTYPE"],
                                                      right_on=["CUI1", "CUI2_SEMTYPE"])  # match by CUIs and semtypes together
    # Overwrite retired OBJECT_* values with new CUI2_* values
    retired_predications["OBJECT_CUI"] = np.where(retired_predications["IS_OBJECT_RETIRED"],
                                                  retired_predications["CUI2"], retired_predications["OBJECT_CUI"])
    retired_predications["OBJECT_NAME"] = np.where(retired_predications["IS_OBJECT_RETIRED"],
                                                   retired_predications["CUI2_NAME"], retired_predications["OBJECT_NAME"])
    retired_predications["OBJECT_SEMTYPE"] = np.where(retired_predications["IS_OBJECT_RETIRED"],
                                                      retired_predications["CUI2_SEMTYPE"], retired_predications["OBJECT_SEMTYPE"])

    # Drop all merged columns from retirement_mapping_data_frame
    retired_predications.drop(columns=retirement_mapping_data_frame.columns, inplace=True)
    # Drop all predications whose retired objects are unmatched
    retired_predications.dropna(axis=0, how="any", subset=["OBJECT_CUI", "OBJECT_NAME", "OBJECT_SEMTYPE"], inplace=True)

    retired_predications = retired_predications.astype({
        "SUBJECT_CUI": "string[pyarrow]",
        "SUBJECT_NAME": "string[pyarrow]",
        "SUBJECT_SEMTYPE": "string[pyarrow]",
        "OBJECT_CUI": "string[pyarrow]",
        "OBJECT_NAME": "string[pyarrow]",
        "OBJECT_SEMTYPE": "string[pyarrow]",
    })

    ##########
    # Step 4 #
    ##########
    # Now these two columns are not necessary. Drop them to save memory
    retired_predications.drop(columns=["IS_SUBJECT_RETIRED", "IS_OBJECT_RETIRED"], inplace=True)
    predication_data_frame.drop(columns=["IS_SUBJECT_RETIRED", "IS_OBJECT_RETIRED"], inplace=True)

    # Drop the original retired predications
    retired_index = predication_data_frame.index[retired_flags]
    predication_data_frame.drop(index=retired_index, inplace=True)

    # Append the matched new predications
    predication_data_frame = pd.concat([predication_data_frame, retired_predications], ignore_index=True, copy=False)
    predication_data_frame.sort_values(by="PREDICATION_ID", ignore_index=True)

    return predication_data_frame


def delete_equivalent_ncbigene_ids(predication_data_frame: pd.DataFrame,
                                   node_normalizer_cache_input: str = None,
                                   node_normalizer_cache_output: str = None):
    def get_cui_to_gene_id_maps(sub_cui_flags: pd.Series, obj_cui_flags: pd.Series, chunk_size=1000):
        sub_cuis = set(predication_data_frame.loc[sub_cui_flags, "SUBJECT_CUI"].unique())
        obj_cuis = set(predication_data_frame.loc[obj_cui_flags, "OBJECT_CUI"].unique())

        if node_normalizer_cache_input and os.path.exists(node_normalizer_cache_input):
            with open(node_normalizer_cache_input, 'rb') as handle:
                cui_gene_id_map = pickle.load(handle)
        else:
            cuis = sub_cuis.union(obj_cuis)
            # a <CUI, Gene_ID> map where there the key is a source CUI and the value is its equivalent NCBIGene ID
            cui_gene_id_map = query_node_normalizer_for_equivalent_ncbigene_ids(cuis, chunk_size=chunk_size)

        # Output to the specified pickle file regardless if it's cache or live response
        if node_normalizer_cache_output:
            with open(node_normalizer_cache_output, 'wb') as handle:
                pickle.dump(cui_gene_id_map, handle, protocol=pickle.HIGHEST_PROTOCOL)

        sub_cui_gene_id_map = {cui: gene_id for cui, gene_id in cui_gene_id_map.items() if cui in sub_cuis}
        obj_cui_gene_id_map = {cui: gene_id for cui, gene_id in cui_gene_id_map.items() if cui in obj_cuis}

        return sub_cui_gene_id_map, obj_cui_gene_id_map

    def get_pred_id_to_cui_maps(sub_cui_flags: pd.Series, obj_cui_flags: pd.Series):
        sub_cui_predications = predication_data_frame.loc[sub_cui_flags, ["SUBJECT_CUI", "PREDICATION_ID"]]
        obj_cui_predications = predication_data_frame.loc[obj_cui_flags, ["OBJECT_CUI", "PREDICATION_ID"]]

        pred_id_sub_cui_map = dict(zip(sub_cui_predications["PREDICATION_ID"], sub_cui_predications["SUBJECT_CUI"]))
        pred_id_obj_cui_map = dict(zip(obj_cui_predications["PREDICATION_ID"], obj_cui_predications["OBJECT_CUI"]))

        return pred_id_sub_cui_map, pred_id_obj_cui_map

    def establish_pred_id_to_gene_id_map(pid_cui_map: Dict, cui_gid_map: Dict):
        pid_gid_map = {pid: cui_gid_map[cui] for pid, cui in pid_cui_map.items() if cui in cui_gid_map}
        return pid_gid_map

    def get_row_index_of_equivalent_ncbigene_ids(pred_id_sub_gene_id_map: Dict, pred_id_obj_gene_id_map: Dict):
        sub_piped_predications = predication_data_frame.loc[predication_data_frame["IS_SUBJECT_PIPED"], ["PREDICATION_ID", "SUBJECT_CUI"]]
        obj_piped_predications = predication_data_frame.loc[predication_data_frame["IS_OBJECT_PIPED"], ["PREDICATION_ID", "OBJECT_CUI"]]

        sub_piped_predications.reset_index(drop=False, inplace=True)  # make the integer index a column named "index"
        obj_piped_predications.reset_index(drop=False, inplace=True)  # make the integer index a column named "index"

        pid_sub_gid_df = pd.DataFrame(data=pred_id_sub_gene_id_map.items(), columns=["PREDICATION_ID", "SUBJECT_CUI"])
        pid_obj_gid_df = pd.DataFrame(data=pred_id_obj_gene_id_map.items(), columns=["PREDICATION_ID", "OBJECT_CUI"])

        dest_sub_gid_df = sub_piped_predications.merge(pid_sub_gid_df, how="inner",
                                                       left_on=["PREDICATION_ID", "SUBJECT_CUI"],
                                                       right_on=["PREDICATION_ID", "SUBJECT_CUI"])
        dest_obj_gid_df = obj_piped_predications.merge(pid_obj_gid_df, how="inner",
                                                       left_on=["PREDICATION_ID", "OBJECT_CUI"],
                                                       right_on=["PREDICATION_ID", "OBJECT_CUI"])

        dest_sub_gid_index = set(dest_sub_gid_df["index"].unique())
        dest_obj_gid_index = set(dest_obj_gid_df["index"].unique())

        dest_gid_index = dest_sub_gid_index.union(dest_obj_gid_index)
        return dest_gid_index

    candidate_sub_cui_flags = predication_data_frame["IS_SUBJECT_PIPED"] & predication_data_frame["SUBJECT_PREFIX"].eq("umls")
    candidate_obj_cui_flags = predication_data_frame["IS_OBJECT_PIPED"] & predication_data_frame["OBJECT_PREFIX"].eq("umls")
    sub_cui_gid_map, obj_cui_gid_map = get_cui_to_gene_id_maps(candidate_sub_cui_flags, candidate_obj_cui_flags, chunk_size=1000)

    source_sub_cui_flags = predication_data_frame["IS_SUBJECT_PIPED"] & predication_data_frame["SUBJECT_CUI"].isin(sub_cui_gid_map)
    source_obj_cui_flags = predication_data_frame["IS_OBJECT_PIPED"] & predication_data_frame["OBJECT_CUI"].isin(obj_cui_gid_map)
    pid_sub_cui_map, pid_obj_cui_map = get_pred_id_to_cui_maps(source_sub_cui_flags, source_obj_cui_flags)

    pid_sub_gid_map = establish_pred_id_to_gene_id_map(pid_sub_cui_map, sub_cui_gid_map)
    pid_obj_gid_map = establish_pred_id_to_gene_id_map(pid_obj_cui_map, obj_cui_gid_map)

    dest_equivalent_gid_index = get_row_index_of_equivalent_ncbigene_ids(pid_sub_gid_map, pid_obj_gid_map)
    predication_data_frame.drop(index=dest_equivalent_gid_index, inplace=True)

    # Now these two columns are not necessary. Drop them to save memory
    predication_data_frame.drop(columns=["IS_SUBJECT_PIPED", "IS_OBJECT_PIPED"], inplace=True)

    return predication_data_frame


def add_document_id_column(predication_data_frame: pd.DataFrame):
    # CUIs in descending order so a true CUI always precedes a NCBIGene ID inside a predication group
    predication_data_frame.sort_values(by=['PREDICATION_ID', 'SUBJECT_CUI', 'OBJECT_CUI'],
                                       ascending=[True, False, False], ignore_index=True, inplace=True)

    pred_groups = predication_data_frame.loc[:, ["PREDICATION_ID"]].groupby("PREDICATION_ID")
    groupwise_pred_nums = pred_groups.cumcount().add(1)
    group_sizes = pred_groups.transform("size")

    primary_ids = predication_data_frame["PREDICATION_ID"].astype("string[pyarrow]")
    secondary_ids = (f"{pid}-{num}" for pid, num in zip(predication_data_frame["PREDICATION_ID"], groupwise_pred_nums))
    secondary_ids = pd.Series(data=secondary_ids, dtype="string[pyarrow]", index=predication_data_frame.index)

    _ids = pd.Series(data=np.where(group_sizes.eq(1), primary_ids, secondary_ids),
                     dtype="string[pyarrow]", index=predication_data_frame.index)
    predication_data_frame["_ID"] = _ids
    return predication_data_frame


def write_semmed_predication_parquet_cache(predication_data_frame: pd.DataFrame, path: str):
    # Option description see https://pandas.pydata.org/pandas-docs/version/1.1/reference/api/pandas.DataFrame.to_parquet.html
    engine = "pyarrow"
    compression = "snappy"

    predication_data_frame.to_parquet(path=path, index=False, engine=engine, compression=compression)


def read_semmed_predication_parquet_cache(path: str) -> pd.DataFrame:
    # Option description see https://pandas.pydata.org/pandas-docs/version/1.1/reference/api/pandas.DataFrame.to_parquet.html
    engine = "pyarrow"
    predication_data_frame = pd.read_parquet(path=path, engine=engine)

    string_columns = ["_ID", "PREDICATE", "SUBJECT_CUI", "SUBJECT_NAME", "SUBJECT_SEMTYPE", "OBJECT_CUI", "OBJECT_NAME", "OBJECT_SEMTYPE"]
    existing_string_columns = [col for col in string_columns if col in predication_data_frame.columns]
    dtype_map = {col: "string[pyarrow]" for col in existing_string_columns}

    predication_data_frame = predication_data_frame.astype(dtype=dtype_map, copy=False)

    return predication_data_frame


##################################
# PART 5: Node Normalizer Client #
##################################

def query_node_normalizer_for_equivalent_ncbigene_ids(cui_collection: Collection, chunk_size: int) -> Dict:
    """
    Given a collection of CUIs, query Node Normalizer to fetch their equivalent NCBIGene IDs.

    To avoid timeout issues, the CUI collection will be partitioned into chunks.
    Each chunk of CUIs will be passed to the Node Normalizer's POST endpoint for querying.
    """

    # Define the querying task for each chunk of CUIs
    def _query(cui_chunk: Collection) -> dict:
        cui_gene_id_map = {}

        cui_prefix = "UMLS:"
        gene_id_prefix = "NCBIGene:"

        url = f"https://nodenorm.transltr.io/get_normalized_nodes"
        payload = {
            # {"conflate": True} means "the conflated data will be returned by the endpoint".
            # See https://github.com/TranslatorSRI/Babel/wiki/Babel-output-formats#conflation
            "conflate": True,
            "curies": [f"{cui_prefix}{cui}" for cui in cui_chunk]
        }

        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        json_resp = resp.json()

        for curie, curie_result in json_resp.items():
            if curie_result is None:
                continue

            for eq_id in curie_result.get("equivalent_identifiers"):
                identifier = eq_id["identifier"]
                if identifier.startswith(gene_id_prefix):
                    cui = curie[len(cui_prefix):]  # trim out the prefix "UMLS:"
                    cui_gene_id_map[cui] = identifier[len(gene_id_prefix):]  # trim out the prefix "NCBIGene:"
                    break

        return cui_gene_id_map

    cui_gene_id_maps = [_query(cui_chunk) for cui_chunk in iter_n(cui_collection, chunk_size)]
    # Merge all dictionaries in "cui_gene_id_maps"
    merged_map = {cui: gene_id for cg_map in cui_gene_id_maps for cui, gene_id in cg_map.items()}

    return merged_map


##################
# PART 6: Parser #
##################

def construct_document(predication: pd.Series, semantic_type_map, semmed_sentence_map):
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
    doc = {
        "_id": predication["_ID"],
        "predication_id": predication["PREDICATION_ID"],
        "pmid": predication["PMID"],
        "predicate": predication["PREDICATE"],
        "sentence": semmed_sentence_map.get(predication["SENTENCE_ID"], None),
        "subject": {
            predication["SUBJECT_PREFIX"]: predication["SUBJECT_CUI"],
            "name": predication["SUBJECT_NAME"],
            "semantic_type_abbreviation": predication["SUBJECT_SEMTYPE"],
            "semantic_type_name": semantic_type_map.get(predication["SUBJECT_SEMTYPE"], None),
            "novelty": predication["SUBJECT_NOVELTY"]
        },
        "object": {
            predication["OBJECT_PREFIX"]: predication["OBJECT_CUI"],
            "name": predication["OBJECT_NAME"],
            "semantic_type_abbreviation": predication["OBJECT_SEMTYPE"],
            "semantic_type_name": semantic_type_map.get(predication["OBJECT_SEMTYPE"], None),
            "novelty": predication["OBJECT_NOVELTY"]
        }
    }

    # del semtype_name field if we did not any mappings
    if not doc["subject"]["semantic_type_name"]:
        del doc["subject"]["semantic_type_name"]
    if not doc["object"]["semantic_type_name"]:
        del doc["object"]["semantic_type_name"]

    return doc


def construct_semmed_predication_data_frame(semmed_predication_filepath,
                                            mrcui_filepath,
                                            umls_cui_name_semtype_filepath,
                                            nord_norm_cache_filepath) -> pd.DataFrame:
    pred_df = read_semmed_predication_data_frame(semmed_predication_filepath)
    pred_df = delete_zero_novelty_scores(pred_df)
    pred_df = delete_invalid_object_cuis(pred_df)
    pred_df = explode_pipes(pred_df)

    mrcui_df = read_mrcui_data_frame(mrcui_filepath)
    deleted_cuis = get_retired_cuis_for_deletion(mrcui_df)
    pred_df = delete_retired_cuis(pred_df, deleted_cuis)

    pred_df = add_prefix_columns(pred_df)
    semmed_cui_name_semtype_df = get_cui_name_and_semtype_from_semmed(pred_df)
    umls_cui_name_semtype_df = read_cui_name_and_semtype_from_umls(umls_cui_name_semtype_filepath)  # pre-generated file; see README.md
    retirement_mapping_df = get_retirement_mapping_data_frame(mrcui_df)
    retirement_mapping_df = add_cui_name_and_semtype_to_retirement_mapping(retirement_mapping_df, semmed_cui_name_semtype_df, umls_cui_name_semtype_df)
    pred_df = map_retired_cuis(pred_df, retirement_mapping_df)

    pred_df = delete_equivalent_ncbigene_ids(pred_df, node_normalizer_cache_input=nord_norm_cache_filepath, node_normalizer_cache_output=None)

    pred_df = add_document_id_column(pred_df)

    return pred_df


def load_data(data_folder, write_semmed_cache=False):
    # Cache filepaths
    semmed_pred_cache_path = os.path.join(data_folder, CACHE_DIR, SEMMED_PREDICATION_CACHE_FN)
    node_norm_cache_path = os.path.join(data_folder, CACHE_DIR, SEMMED_NODE_NORM_RESPONSE_CACHE_FN)
    # SemMedDB filepaths
    semmed_pred_path = os.path.join(data_folder, SEMMED_PREDICATION_FN)
    semmed_sentence_path = os.path.join(data_folder, SEMMED_SENTENCE_FN)
    # Auxiliary filepaths
    mrcui_path = os.path.join(data_folder, UMLS_METATHESAURUS_DIR, MRCUI_FN)
    umls_cui_name_semtype_path = os.path.join(data_folder, UMLS_PREFERRED_CUI_NAME_SEMTYPE_FN)
    semtype_mapping_path = os.path.join(data_folder, SEMTYPE_MAPPING_FN)

    # Always read the cache if available
    if semmed_pred_cache_path and os.path.exists(semmed_pred_cache_path):
        semmed_pred_df = read_semmed_predication_parquet_cache(path=semmed_pred_cache_path)
    else:
        # Start the data cleaning procedure if cache not available
        semmed_pred_df = construct_semmed_predication_data_frame(semmed_predication_filepath=semmed_pred_path,
                                                                 mrcui_filepath=mrcui_path,
                                                                 umls_cui_name_semtype_filepath=umls_cui_name_semtype_path,
                                                                 nord_norm_cache_filepath=node_norm_cache_path)

    # Write cache only when `write_semmed_cache` is set
    if write_semmed_cache:
        write_semmed_predication_parquet_cache(semmed_pred_df, path=semmed_pred_cache_path)

    semtype_mappings_df = read_semantic_type_mappings_data_frame(filepath=semtype_mapping_path)
    semtype_name_map = get_semtype_name_map(semtype_mappings_df)

    semmed_sentence_df = read_semmed_sentence_data_frame(filepath=semmed_sentence_path)
    semmed_sentence_map = get_semmed_sentence_map(semmed_sentence_df, semmed_pred_df)

    for _, row in semmed_pred_df.iterrows():
        doc = construct_document(row, semtype_name_map, semmed_sentence_map)
        yield doc
