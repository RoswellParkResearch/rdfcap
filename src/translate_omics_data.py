from src.ontology_class.data_entity_ontology_generated_class import data_entity_ontology_dev
from src.util.uri_util import *
from rdflib import Graph, RDF, RDFS, OWL, Namespace, BNode, URIRef, Literal, XSD
import pandas as pds
import sys

reload(sys)
sys.setdefaultencoding('utf8')


def generate_structure_ontology(df, data_namespace_uri, data_source, owl_file=""):
    # ontology class
    dent = data_entity_ontology_dev()

    # build graph
    if len(owl_file.strip()) > 0:
        graph = Graph(identifier=data_namespace_uri)
        graph.parse(owl_file)
    else:
        graph = Graph(identifier=data_namespace_uri)

    # declare project/data source as a class (data collection as parent)
    project_uri = nested_uri(make_instance_uri(dent.data_collection_uri),
                             data_source
                            )

    dent.declare_class(graph,
                       project_uri,
                       dent.data_collection_uri,
                       label=data_source
                       )

    dent.declare_data_property(graph,
                               nested_uri(make_instance_uri(dent.record_value_uri),
                                          data_source
                                          ),
                               dent.record_value_uri,
                               label='%s DP'
                               )

    # columns as data properties
    for column in df.columns:
        dent.declare_data_property(graph,
                                   nested_uri(make_instance_uri(dent.record_value_uri),
                                              data_source,
                                              column
                                             ),
                                   nested_uri(make_instance_uri(dent.record_value_uri),
                                              data_source
                                              ),
                                   label='%s DP' % str(column).replace('_', ' ')
                                   )
    return graph


def generate_record_id(id_keys, row):
    record_id = '%s'
    id_vals = []
    if len(id_keys) > 0:
        for key in id_keys:
            record_id += '_%s'
            id_vals.append(row[key])
        id_vals.append(row['Index'])
        return record_id % tuple(id_vals)
    return record_id % row['Index']


def translate_raw_data(df, data_namespace_uri, data_source, id_keys=[], owl_file=""):
    # ontology class
    dent = data_entity_ontology_dev()

    # build graph
    if len(owl_file.strip()) > 0:
        graph = Graph(identifier=data_namespace_uri)
        graph.parse(owl_file)
    else:
        graph = Graph(identifier=data_namespace_uri)

    project_uri = nested_uri(make_instance_uri(dent.data_collection_uri),
                             data_source
                             )

    # record id generation. can subset any fields to use as unique IDs
    # row[0] in pandas dataframe is always Index
    for row in df[id_keys].itertuples():
        row = row.__dict__
        record_id = generate_record_id(id_keys, row)

        dent.declare_individual(graph,
                                nested_uri(make_instance_uri(dent.data_collection_uri),
                                           data_source,
                                           record_id),
                                project_uri,
                                label=str(record_id).replace('_', ' ')
                                )

    for row in df.itertuples():
        row = row.__dict__

        record_id = generate_record_id(id_keys, row)
        record_uri = nested_uri(make_instance_uri(dent.data_collection_uri),
                                data_source,
                                record_id
                                )

        for col, val in row.items():
            if col != 'Index':
                field_uri = nested_uri(make_instance_uri(dent.record_value_uri),
                                       data_source,
                                       col
                                       )
                graph.add((record_uri, field_uri, Literal(val)))
    return graph


if __name__ == "__main__":
    path_in = r'/Users/ph37399/projects/Omics DB translation/RDTA_All Omics data'
    df = pds.read_csv(path_in, sep='\t')

    graph_structure = generate_structure_ontology(df, # raw dataframe
                                                  'http://purl.roswellpark.org/ontology/omics',  # namespace uri
                                                  'omics_db', # data_source
                                                  owl_file=r'/Users/ph37399/projects/Omics DB translation/rdfcap-merged.owl',
                                                  )
    graph_instance_data = translate_raw_data(df,
                                             'http://purl.roswellpark.org/ontology/omics',  # namespace uri
                                             'omics_db', # data_source
                                             id_keys=['mrn', 'alias_report_id'],
                                             owl_file=r'/Users/ph37399/projects/Omics DB translation/rdfcap-merged.owl',
                                             )

    with open('/Users/ph37399/projects/Omics DB translation/Omics ontology structure.owl', 'w') as f:
        f.write(graph_structure.serialize(format='xml'))
    f.close()

    with open('/Users/ph37399/projects/Omics DB translation/Omics instance data.owl', 'w') as f:
        f.write(graph_instance_data.serialize(format='xml'))
    f.close()