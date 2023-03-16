def semmeddb_prediction_mapping(cls):
    mapping = {
        "predicate": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "predication_id": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "pmid": {
            "type": "integer"
        },
        "sentence": {
            "type": "text"
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
