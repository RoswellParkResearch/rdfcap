from src.generate_ontology_class import build_ontology_functions

build_ontology_functions('../ontology/dev/rdfcap-dev.owl',
                         pyfile_name="../ontology_class/rdfcap_base_generated_class.py",
                         print_output=True)