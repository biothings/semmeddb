from parser import *

if __name__ == '__main__':
    data_folder = "/data/pending/datasources/semmeddb/43"

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

    print(f"[Source] SemMedDB Predication Table: {semmed_pred_path}")
    print(f"[Source] SemMedDB Sentence Table: {semmed_sentence_path}")

    print(f"[Cache] SemMedDB Predication Cache: {semmed_pred_cache_path}")
    print(f"[Cache] SemMedDB NordNorm Cache: {node_norm_cache_path}")

    print(f"[Aux] UMLS Metathesaurus MRCUI: {mrcui_path}")
    print(f"[Aux] UMLS Preferred CUI Names & Semtypes: {umls_cui_name_semtype_path}")
    print(f"[Aux] UMLS Semtype Mapping: {semtype_mapping_path}")

    if semmed_pred_cache_path and os.path.exists(semmed_pred_cache_path):
        print(f"[Cache] SemMedDB Predication Cache {semmed_pred_cache_path} exists.")
    else:
        print(f"[Cache] SemMedDB Predication Cache {semmed_pred_cache_path} does not exist.")

    semmed_pred_df = construct_semmed_predication_data_frame(semmed_predication_filepath=semmed_pred_path,
                                                             mrcui_filepath=mrcui_path,
                                                             umls_cui_name_semtype_filepath=umls_cui_name_semtype_path,
                                                             node_norm_cache_filepath=node_norm_cache_path,
                                                             write_node_norm_cache=True)

    print(f"[Cache] SemMedDB Predication Cache {semmed_pred_cache_path} rewriting...")
    write_semmed_predication_parquet_cache(semmed_pred_df, path=semmed_pred_cache_path)


