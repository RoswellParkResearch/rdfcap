from src.ontology_class.rdfcap_base_generated_class import rdfcap_dev
from src.ontology_class.data_entity_ontology_generated_class import data_entity_ontology_dev
from src.generate_project_data import *
from src.convert_meta_eav import *
from src.util.translation_operations import *
from src.util.uri_util import *
from rdflib import Graph, RDF, RDFS, OWL, Namespace, BNode, URIRef, Literal, XSD
import pandas as pds

def translate_metadata(data_namespace_uri, project_data, meta_data, owl_file):
    # ontology classes
    rdfcap = rdfcap_dev()
    dent = data_entity_ontology_dev()

    # build graph
    if len(owl_file.strip()) > 0:
        graph = Graph(identifier=data_namespace_uri)
        graph.parse(owl_file)
    else:
        graph = Graph(identifier=data_namespace_uri)

    # generate project data
    project_meta = generate_project_metadata(project_data)

    metadata = pds.read_csv(meta_data, sep='\t')

    projects = []
    for row in metadata.itertuples():
        row = row.__dict__

        # something odd happened in the metadata df so handle it
        try:
            pid = int(row['project_id'])
        except ValueError:
            continue

        field_name = str(row['field_name']).strip()
        form_name = str(row['form_name']).strip()
        field_label = str(row['element_label']).strip()
        field_order = str(row['field_order']).strip()
        element_type = str(row['element_type']).strip()
        pid = str(row['project_id']).replace('.0', '').strip()

        enum_val = str(row['enum_value']).strip() if str(row['enum_value']).strip() != "nan" else ""
        enum_label = str(row['enum_label']).strip() if str(row['enum_label']).strip() != "nan" else ""

        # project field subclass of data field
        if pid not in projects:
            rdfcap.declare_class(graph,
                                 rdfcap.REDCap_metadata_record(project_meta[pid]['project_name']),
                                 rdfcap.REDCap_metadata_record_uri,
                                 label='%s metadata field' % project_meta[pid]['app_title']
                                 )
            projects.append(pid)

        # meta data relations
        instance_num = '/%s' % enum_val if enum_val != "" else ""
        field_uri = rdfcap.REDCap_metadata_record('%s/%s%s' % (project_meta[pid]['project_name'], field_name, instance_num))
        rdfcap.declare_individual(graph,
                                  field_uri,
                                  rdfcap.REDCap_metadata_record(project_meta[pid]['project_name']),
                                  label='%s - %s %s' % (project_meta[pid]['app_title'],
                                                        field_label,
                                                        enum_val
                                                        )
                                  )
        rdfcap.field_name(graph,
                          field_uri,
                          field_name
                          )
        rdfcap.project_id(graph,
                          field_uri,
                          pid
                          )
        dent.field_of(graph,
                      field_uri,
                      rdfcap.REDCap_project(project_meta[pid]['project_name']))
        rdfcap.project_name(graph,
                            field_uri,
                            project_meta[pid]['project_name']
                            )
        rdfcap.form_name(graph,
                         field_uri,
                         form_name
                         )
        rdfcap.element_label(graph,
                             field_uri,
                             field_label
                             )
        rdfcap.field_order(graph,
                           field_uri,
                           field_order
                           )
        rdfcap.element_type(graph,
                            field_uri,
                            element_type
                            )
        rdfcap.specifies_iri(graph,
                             field_uri,
                             nested_uri(make_instance_uri(dent.record_value_uri),
                                        project_meta[pid]['project_name'],
                                        form_name,
                                        field_name
                                        )
                             )
        if enum_val != "":
            rdfcap.enum_value(graph,
                              field_uri,
                              enum_val
                             )
        if enum_label != "":
            rdfcap.enum_label(graph,
                              field_uri,
                              enum_label
                              )

    return graph

if __name__ == "__main__":

    graph = translate_metadata('http://purl.roswellpark.org/ontology/rdfcap', # namespace uri
                               r'Z:\Projects\Phil Whalen\RDFCap\Raw Data\LST_RC Projects.txt', # project data
                               r'W:\processed_225 135 67 133 293 metadata.txt', # redcap metadata
                               owl_file=r'Z:\Projects\Phil Whalen\RDFCap\rdfcap-merged.owl')
    print graph.serialize(format='turtle')

    with open('W:/metadata_225 135 67 133 293.owl', 'w') as f:
        f.write(graph.serialize(format='xml'))
    f.close()