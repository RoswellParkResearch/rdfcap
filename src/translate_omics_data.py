from src.ontology_class.data_entity_ontology_generated_class import data_entity_ontology_dev
from src.util.uri_util import *
from rdflib import Graph, RDF, RDFS, OWL, Namespace, BNode, URIRef, Literal, XSD
import pandas as pds
import sys

reload(sys)
sys.setdefaultencoding('utf8')

def translate_raw_omics_data(df, data_namespace_uri, owl_file=""):
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
                             'omics'
                            )

    dent.declare_class(graph,
                       project_uri,
                       dent.data_collection_uri,
                       label='Omics Database'
                       )

    # columns as data properties
    for column in df.columns:
        dent.declare_data_property(graph,
                                   nested_uri(make_instance_uri(dent.record_value_uri),
                                              column
                                             ),
                                   dent.record_value_uri,
                                   label=str(column).replace('_', ' ')
                                   )

    # record id generation. can subset any fields to use as unique IDs
    for row in df[['mrn', 'alias_report_id']].itertuples():
        record_id = '%s_%s_%s' % (row[1], str(row[2]).replace('-',''), row[0])

        dent.declare_individual(graph,
                                nested_uri(make_instance_uri(dent.data_collection_uri),
                                           'omics',
                                           record_id),
                                project_uri,
                                label=record_id
                                )

    for row in df.itertuples():
        row = row.__dict__

        record_id = '%s_%s_%s' % (row['mrn'], str(row['alias_report_id']).replace('-', ''), row['Index'])
        record_uri = nested_uri(make_instance_uri(dent.data_collection_uri),
                                'omics',
                                record_id
                                )

        for col, val in row.items():
            if col != 'Index':
                field_uri = nested_uri(make_instance_uri(dent.record_value_uri),
                                       col
                                       )
                graph.add((record_uri, field_uri, Literal(val)))

    return graph


if __name__ == "__main__":
    path_in = r'/Users/ph37399/projects/Omics DB translation/RDTA_All Omics data'
    df = pds.read_csv(path_in, sep='\t')

    graph = translate_raw_omics_data(df, # raw dataframe
                                     'http://purl.roswellpark.org/ontology/omics',  # namespace uri
                                     owl_file=r'/Users/ph37399/projects/Omics DB translation/rdfcap-merged.owl',
                               )

    print graph.serialize(format='turtle')