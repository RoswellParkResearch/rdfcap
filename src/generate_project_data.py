import pandas as pds
from src.ontology_class.rdfcap_base_generated_class import rdfcap_dev
from rdflib import Graph, RDF, RDFS, OWL, Namespace, BNode, URIRef, Literal, XSD
from src.util.uri_util import *

def generate_project_data(project_data, graph, data_namespace_uri):
    rdfcap = rdfcap_dev()
    df = pds.read_csv(project_data, sep='\t')

    data = Namespace(parse_base_uri(data_namespace_uri))  # base uri
    for row in df.itertuples():
        row = row.__dict__

        id = str(row['project_id']).replace('.0', '')
        name = str(row['project_name'])
        label = str(row['app_title'])

        # build graph
        record_uri = nested_uri(make_instance_uri(rdfcap.REDCap_project_uri),
                                name
                                )
        rdfcap.declare_individual(graph,
                                  record_uri,
                                  rdfcap.REDCap_project_uri,
                                  label=label)

        rdfcap.project_id(graph, record_uri, id)
        rdfcap.project_name(graph, record_uri, name)
        rdfcap.application_title(graph, record_uri, label)

    return graph


def generate_project_metadata(project_data):
    df = pds.read_csv(project_data, sep='\t')

    project_meta = {}
    for row in df.itertuples():
        row = row.__dict__

        id = str(row['project_id']).replace('.0', '')
        name = str(row['project_name'])
        label = str(row['app_title'])

        # build meta dictionary
        if id not in project_meta:
            project_meta[id] = {}
        project_meta[id]["project_name"] = name
        project_meta[id]["app_title"] = label
    return project_meta

if __name__ == "__main__":
    project_data = r'Z:\Projects\Phil Whalen\RDFCap\Raw Data\LST_RC Projects.txt'
    graph = Graph(identifier="http://purl.roswellpark.org/ontology/rdfcap#")

    graph, project_meta = generate_project_data(project_data, graph, data_namespace_uri=graph.identifier)

    print graph.serialize(format='turtle')