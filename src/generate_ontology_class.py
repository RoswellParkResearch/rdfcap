# coding=utf-8
import rdflib
from rdflib import URIRef, RDF, OWL, Graph
import re


to_initialize = []

def data_source_uri(datasrcuri):

    datasrcuri = URIRef(datasrcuri)

    return datasrcuri

def format_as_python_name(name):
    py_name = name.replace(' ', '_')
    py_name = py_name.replace('.owl', '')
    py_name = py_name.replace('.rdf', '')
    py_name = py_name.replace('.ttl', '')
    py_name = py_name.replace('.trig', '')
    py_name = py_name.replace('.xml', '')
    py_name = py_name.replace('.', '_')
    py_name = py_name.replace('-', '_')
    py_name = py_name.replace('+', '_')
    py_name = py_name.replace('=', '_')
    py_name = py_name.replace('!', '_')
    py_name = py_name.replace(',', '_')
    py_name = py_name.replace('*', '_')
    py_name = py_name.replace('@', '_')
    py_name = py_name.replace('&', '_')
    py_name = py_name.replace('$', '_')
    py_name = py_name.replace('~', '_')
    py_name = py_name.replace('?', '_')
    py_name = py_name.replace(':', '_')
    py_name = py_name.replace('%20', '_')
    py_name = re.sub(r'[^a-z,A-Z,0-9,_]', '', py_name)
    return py_name


def ttl_function_object_property(function_name, prop_uri, datatype=""):
    function_string = """
    def {function_name}(self, uri1, uri2):
        return "%s <{prop_uri}> %s . \\n" % (uri1, uri2)
        """.format(function_name=function_name, prop_uri=prop_uri)
    function_string += """
    self.{function_name}_uri = "<{prop_uri}>"
            """.format(function_name=function_name, prop_uri=str(prop_uri))
    function_string += """
    self.{function_name}_label = "{function_name}"
            """.format(function_name=function_name, prop_uri=str(function_name))
    return function_string


def ttl_function_data_property(function_name, prop_uri, datatype=""):
    function_string = """
    def {function_name}(self, uri1, uri2):
        return "%s <{prop_uri}> %s . \\n" % (uri1, uri2)
        """.format(function_name=function_name, prop_uri=prop_uri)
    to_initialize.append('{function_name}_uri = "<{prop_uri}>"'.format(function_name=function_name, prop_uri=str(prop_uri)))
    to_initialize.append('self.{function_name}_label = "{function_name}"'.format(function_name=function_name, prop_uri=str(function_name)))
    return function_string


def ttl_function_annotation_property(function_name, prop_uri, datatype=""):
    function_string = """
    def {function_name}(self, uri1, uri2):
        return "%s <{prop_uri}> %s . \\n" % (uri1, uri2)
        """.format(function_name=function_name, prop_uri=prop_uri)
    to_initialize.append('self.{function_name}_uri = "<{prop_uri}>"'.format(function_name=function_name, prop_uri=str(prop_uri)))
    to_initialize.append('self.{function_name}_label = "{function_name}"'.format(function_name=function_name, prop_uri=str(function_name)))
    return function_string


def ttl_function_class(class_name, class_uri):
    function_string = """
    def {class_name}(self, id):
        return "<{class_uri}/%s>" % id.strip()
        """.format(class_name=class_name, class_uri=class_uri)
    to_initialize.append('self.{class_name}_uri = "<{class_uri}>"'.format(class_name=class_name, class_uri=str(class_uri)))
    to_initialize.append('self.{class_name}_label = "{class_name}"'.format(class_name=class_name))
    return function_string



def rdflib_function_object_property(function_name, prop_uri):
    function_string =    """
    def {function_name}(self,graph,uri1,uri2):
        uri1 = URIRef(uri1)
        uri2 = URIRef(uri2)
        graph.add((uri1,URIRef("{prop_uri}"),uri2))
        return graph
        """.format(function_name=function_name, prop_uri=prop_uri)
    to_initialize.append('self.{function_name}_uri = URIRef("{prop_uri}")'.format(function_name=function_name, prop_uri=str(prop_uri)))
    to_initialize.append('self.{function_name}_label = "{function_name}"'.format(function_name=function_name, prop_uri=str(function_name)))
    return function_string



def rdflib_function_data_property(function_name, prop_uri):
    function_string =    """
    def {function_name}(self,graph,uri1,value):
        uri1 = URIRef(uri1)
        graph.add((uri1,URIRef("{prop_uri}"),Literal(value)))
        return graph
        """.format(function_name=function_name, prop_uri=prop_uri)
    to_initialize.append('self.{function_name}_uri = URIRef("{prop_uri}")'.format(function_name=function_name, prop_uri=str(prop_uri)))
    to_initialize.append('self.{function_name}_label = "{function_name}"'.format(function_name=function_name, prop_uri=str(function_name)))
    return function_string


def rdflib_function_annotation_property(function_name, prop_uri):
    function_string =    """
    def {function_name}(self,graph,uri1,value="", uri2=""):
        uri1 = URIRef(uri1)
        if len(value) > 0:
            graph.add((uri1,URIRef("{prop_uri}"),value))
        else:
            uri2 = URIRef(uri2)
            graph.add((uri1,URIRef("{prop_uri}"),uri2))
        return graph
        """.format(function_name=function_name, prop_uri=prop_uri)
    to_initialize.append('self.{function_name}_uri = URIRef("{prop_uri}")'.format(function_name=function_name, prop_uri=str(prop_uri)))
    to_initialize.append('self.{function_name}_label = "{function_name}"'.format(function_name=function_name, prop_uri=str(function_name)))
    return function_string


def rdflib_function_class(class_name, class_uri):
    function_string = """
    def {class_name}(self,id):
        uri = "{class_uri}/%s" % id.strip()
        return URIRef(uri) 
        """.format(class_name=class_name, class_uri=class_uri)
    to_initialize.append('self.{class_name}_uri = URIRef("{class_uri}")'.format(class_name=class_name, class_uri=str(class_uri)))
    to_initialize.append('self.{class_name}_label = "{class_name}"'.format(class_name=class_name))
    return function_string


def format_uri_as_label(uri, make_lower=True):
    # use the last portion of the uri as label
    # use encode('ascii', 'ignore') to ignore unicode characters
    if '#' in str(uri):  # check if uri uses '#' syntax
        short_uri = str(uri.encode('ascii', 'ignore')).split('#')[-1]
    else:
        short_uri = str(uri.encode('ascii', 'ignore')).split('/')[-1]

    if make_lower:
        return format_as_python_name(short_uri.lower())
    else:
        return format_as_python_name(short_uri)


def get_label(graph, uri, make_lower):
    label = str(graph.label(uri)) # get rdfs:label for uri
    if len(label.strip()) < 1: # if no label, use last part of uri
        label = format_uri_as_label(uri, make_lower)
    return label


def format_generated_functions_file_name(ontology_source, pyfile_name="", make_lower=True, function_type="rdflib"):
    # specify name of the python file name for generated functions
    pyfile_name = pyfile_name.strip()
    if len(pyfile_name) > 0:
        pyfile = pyfile_name
    else:
        ontology = format_uri_as_label(ontology_source.strip(), make_lower=make_lower)
        pyfile = "{0}_generated_class_{1}.py".format(format_as_python_name(ontology), function_type.strip())

    return pyfile


def generate_function_object_property(field_name, property, function_type="rdflib"):
    if function_type.strip() == "rdflib":
        return rdflib_function_object_property(field_name, property)
    else:
        return ttl_function_object_property(field_name, property)


def generate_function_data_property(field_name, property, function_type="rdflib"):
    if function_type.strip() == "rdflib":
        return rdflib_function_data_property(field_name, property)
    else:
        return ttl_function_data_property(field_name, property)


def generate_function_annotation_property(field_name, property, function_type="rdflib"):
    if function_type.strip() == "rdflib":
        return rdflib_function_annotation_property(field_name, property)
    else:
        return ttl_function_annotation_property(field_name, property)


def generate_function_class(class_name, class_uri, function_type="rdflib"):
    if function_type.strip() == "rdflib":
        return rdflib_function_class(class_name, class_uri)
    else:
        return ttl_function_object_property(class_name, class_uri)



def generate_common_ontology_functions(funciton_type="rdflib"):
    filename = ""
    if funciton_type.strip() == "rdflib":
        filename = "common_ontology_class_rdflib.py"
    else:
        filename = "common_ontology_class_ttl.py"

    with open(filename, 'r') as f:
        return f.read()

def generate_class_text(ontology_uri):
    ontology = str(ontology_uri).split('/')
    ontology = str(ontology[len(ontology)-1]).replace('.owl', '')

    text_out = """
class {0}(CommonOntology):
    def __init__(self):
        super({0}, self).__init__()
        self.name = "{1}"
        self.initialize_attributes()
                      """.format(format_as_python_name(ontology), ontology)
    return text_out

def generate_setter_function(attributes):
    function_string = """
    def initialize_attributes(self):\n"""
    for attribute in attributes:
        function_string += "\t\t{0}\n".format(attribute)
    return function_string

def build_ontology_functions(ontology_source,
                             pyfile_name="",
                             print_output=False,
                             save_output=True,
                             make_lower=True,
                             function_type="rdflib"):

    def output(output_string):
        if save_output: f.write(output_string)  # write output to file
        if print_output: print(output_string)  # print output

    # specify name of the output file
    pyfile = \
        format_generated_functions_file_name(ontology_source,
                                             pyfile_name=pyfile_name,
                                             make_lower=make_lower,
                                             function_type=function_type)

    # build graph
    graph = Graph()
    graph.parse(ontology_source)

    with open(pyfile, 'w') as f:
        # output the BaseOntology class with common functions
        output(generate_common_ontology_functions(function_type))
        output('\n')
        output(generate_class_text(ontology_source))
        # create a function for each object property
        output("\n################# object properties #################\n")

        for property in graph.subjects(RDF.type, OWL.ObjectProperty):  # query for object properties
            field_name = format_as_python_name(get_label(graph, property, make_lower))  # create function property name
            function_string = generate_function_object_property(field_name, property, function_type)  # create python function property string
            output(function_string)

        # create a function for each data property
        output("\n################# data properties #################\n")
        for property in graph.subjects(RDF.type, OWL.DatatypeProperty): # query for data properties
            field_name = format_as_python_name(get_label(graph, property, make_lower))  # create function property name
            function_string = generate_function_data_property(field_name, property, function_type)  # create python function property string
            output(function_string)

        # create a function for each annotation property
        output("\n################# annotation properties #################\n")
        for property in graph.subjects(RDF.type, OWL.AnnotationProperty):  # query for annotation properties
            field_name = format_as_python_name(get_label(graph, property, make_lower))  # create function property name
            function_string = generate_function_annotation_property(field_name, property, function_type)  # create python function property string
            output(function_string)

        # create a function for each class that returns a uri for an individual / instance
        output("\n################# classes #################\n")
        for class_uri in graph.subjects(RDF.type, OWL.Class): # query for classes
            if type(class_uri) != rdflib.term.BNode:
                class_name = format_as_python_name(get_label(graph, class_uri, make_lower))  # create class name
                uri_string = generate_function_class(class_name, class_uri, function_type)
                output(uri_string)

        # add instance attribute setter function
        output(generate_setter_function(to_initialize))


if __name__ == "__main__":
    build_ontology_functions('ontology/dev/ncit.owl', print_output=True)

