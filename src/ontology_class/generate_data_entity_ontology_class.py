from src.generate_ontology_class import build_ontology_functions

ontology = r'src/ontology/dev/data-entity-ontology-dev.owl'

build_ontology_functions(ontology,
                         pyfile_name="src/ontology_class/data_entity_ontology_generated_class.py",
                         print_output=True)