def semmeddb_prediction_mapping(cls):
    mapping = {
        "predicate": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "pmid_count": {
            "type": "integer"
        },
        "predication_count": {
            "type": "integer"
        },
        "predication": {
            "properties": {
                "predication_id": {
                    "type": "keyword"
                },
                "pmid": {
                    "type": "keyword"
                },
                "sentence_id": {
                    "type": "keyword"
                },
                "sentence": {
                    "type": "text",
                    "index": False
                },
                "subject_text": {
                    "type": "text"
                },
                "subject_score": {
                    "type": "integer"
                },
                "object_text": {
                    "type": "text"
                },
                "object_score": {
                    "type": "integer"
                }
            }
        },
        "subject": {
            "properties": {
                "umls": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "name": {
                    "type": "text"
                },
                "semantic_type_abbreviation": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "semantic_type_name": {
                    "type": "text"
                },
                "novelty": {
                    "type": "integer"
                },
                "ncbigene": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                }
            }
        },
        "object": {
            "properties": {
                "umls": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "name": {
                    "type": "text"
                },
                "semantic_type_abbreviation": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "semantic_type_name": {
                    "type": "text"
                },
                "novelty": {
                    "type": "integer"
                },
                "ncbigene": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                }
            }
        }
    }

    return mapping
