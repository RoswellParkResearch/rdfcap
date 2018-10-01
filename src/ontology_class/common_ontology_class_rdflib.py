from textwrap import dedent
from rdflib import Graph,URIRef,RDF,OWL,RDFS, Literal
import re

################# functions from common_ontology_functions_rdflib.py ###################

class CommonOntology(object):
    def __init__(self):
        self.name = "CommonOntology - RDFLib"

    #Function to declare individual
    def declare_individual(self, graph, uri, class_uri,label=""):

        uri =  URIRef(uri)
        class_uri = URIRef(class_uri)

        #Create graph of URI
        graph.add((uri, RDF.type, OWL.NamedIndividual))
        graph.add((uri, RDF.type, class_uri))

        if len(label.strip()) > 0:
            graph.add((uri, RDFS.label, Literal(label)))

        return graph

    #Function to generate a triple
    def triple(self, graph, subj,pred,obj):

        subj = URIRef(subj)
        pred = URIRef(pred)
        obj = URIRef(obj)

        #Create a graph of a triple with subject, predicate and object
        graph.add((subj, pred, obj))

        return graph

    #Function to declare a class
    def declare_class(self, graph, class_uri,parent_uri,label=""):

        class_uri = URIRef(class_uri)
        parent_uri = URIRef(parent_uri)

        #Declaring an OWL Class
        graph.add((class_uri, RDF.type, OWL.Class))
        graph.add((class_uri, RDFS.subClassOf, parent_uri))

        if len(label.strip()) > 0:
            graph.add((class_uri, RDFS.label, Literal(label)))


        return graph

    #Function to declare Object property
    def declare_object_property(self, graph, prop_uri,parent_uri,label=""):

        prop_uri = URIRef(prop_uri)

        #Declaring an object property
        graph.add((prop_uri, RDF.type, OWL.ObjectProperty))

        if len(parent_uri.strip()) > 0:
            graph.add((prop_uri, RDFS.subPropertyOf, parent_uri))

        if len(label.strip()) > 0:
            graph.add((prop_uri, RDFS.label, Literal(label)))

        return graph

    # Function to declare Data property
    def declare_data_property(self, graph, prop_uri,parent_uri,label=""):
        prop_uri = URIRef(prop_uri)

        # Declaring an data property
        graph.add((prop_uri, RDF.type, OWL.DatatypeProperty))

        if len(parent_uri.strip()) > 0:
            graph.add((prop_uri, RDFS.subPropertyOf, parent_uri))

        if len(label.strip()) > 0:
            graph.add((prop_uri, RDFS.label, Literal(label)))

        return graph
