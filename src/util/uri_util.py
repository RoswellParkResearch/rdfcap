import os
import rdflib
from rdflib import ConjunctiveGraph, URIRef, RDFS, Literal, RDF, OWL, BNode
from datetime import datetime
import pandas as pds
import re
import string

def make_uri_map(filename):
    # build graph
    g = rdflib.Graph()
    g.parse(filename)

    # add mapping: lowercase short uri / label -> uri
    uri_map = {}
    for subj in g.subjects(RDF.type, OWL.Class):
        if type(subj) != rdflib.term.BNode:
            # use encode('ascii', 'ignore') to ignore unicode characters
            short_uri = str(subj.encode('ascii', 'ignore')).lower().split('/')[-1]
            uri_map[short_uri] = str(subj.encode('ascii', 'ignore'))

    # g.parse(filename)
    # print  g.triples( (None, None, None) )
    # for subj, obj in g.subject_objects(RDFS.label):
    for subj, pred, obj in g.triples((None, None, None)):
            # g.query("select ?subj ?pred ?obj where {?subj <http://www.w3.org/2000/01/rdf-schema#label> ?obj .}"):
            # g.triples((None, None, None)):
            # g.query("select ?subj ?pred ?obj where { ?subj rdfs:label ?obj .} limit 100"):

        print (subj, pred, obj)
        labelx = str(obj.encode('ascii', 'ignore')).lower()
        print labelx
        uri_map[labelx] = str(subj.encode('ascii', 'ignore'))

    return uri_map

def write_uri_map(uri_map, filename='uri_map.txt'):
    # save label2uri to file
    with open(filename, 'w') as f:
        f.write(str(uri_map)) # note: uri_map is converted to string


def load_uri_map(force=False, filepath=__file__, filename='uri_map.txt'):
    # create and the lable2uri under the following two coditions:
    # the file does NOT exist OR force is True
    # uri_map_full_name = os.path.join(filepath, filename)
    uri_map_full_name = os.path.join(os.path.abspath('.'), filename)
    if force == True or os.path.exists(uri_map_full_name) == False:
        # print "creating map"
        # make the uri_map map
        uri_map = make_uri_map()

        # write uri_map to file
        write_uri_map(uri_map, uri_map_full_name)

    # otherwise read uri_map from file
    else:
        # print "load from file"
        uri_map = eval(open(uri_map_full_name).read())

    # return uri_map
    return uri_map

def make_uri(base_uri, entity_uri="", base_end_char="/"):
    """:returns a URIRef based on the base uri and entity uri"""

    base_uri = parse_base_uri(base_uri, base_end_char)
    if len(str(entity_uri).strip()) > 0:
        uri = "%s%s" % (str(base_uri), str(entity_uri).strip())
        return URIRef(uri.strip())
    else:
        return URIRef(str(base_uri).strip())


def parse_base_uri(base_uri, base_end_char="/"):
    """formats the base uri so that it ends with a '/','#', or the end_char"""
    if str(base_uri).endswith("/") or str(base_uri).endswith("#"):
        return base_uri
    else:
        return "{0}{1}".format(str(base_uri).strip(), str(base_end_char).strip())


def strip_extension(name):
    """character's after the first '.' in name"""
    return  name.split('.') [0]


def parse_python_name(name):
    """parses name into valid python syntax"""
    py_name = name.replace(' ', '_')
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


def parse_uri_as_label(uri, make_lower=True, make_upper=False, replace_underscore=False):
    """
    for uris that have label like ending (e.g., http://example.com/foo_bar)
    return the last portion (e.g., foo_bar)
    if replace_underscore is true, the underscrores are replaced with sapces (e.g., foo bar)
    """
    # use the last portion of the uri as label
    # use encode('ascii', 'ignore') to ignore unicode characters
    if '#' in str(uri):  # check if uri uses '#' syntax
        short_uri = str(uri.encode('ascii', 'ignore')).split('#')[-1]
    else:
        short_uri = str(uri.encode('ascii', 'ignore')).split('/')[-1]

    # determine underscore usage
    if replace_underscore:
        short_uri = short_uri.replace("_", " ")

    # determine case
    if make_lower:
        short_uri = parse_python_name(short_uri.lower())
    elif make_upper:
        short_uri = parse_python_name(short_uri.upper())

    return short_uri


def parse_file_name(data_file, remove_extension=False):
    """
    returns the last part of a file name (e.g., /user/data/foo.xml -> foo.xml)
    if remove_extension is True, the characters after the last period are removed
    """
    file_name = str(data_file.encode('ascii', 'ignore')).strip().split('/')[-1]
    if remove_extension:
        file_name = strip_extension(file_name)

    return file_name


def make_instance_uri(uri):
    return str(uri).replace('#', '/')



def equivalence_axiom(class_uri, data_property_uri):
    """
    An alternative method for equivalence axioms:

    # create diagnosis and fatigue URIRefs
    diagnosis = URIRef("http://purl.roswellpark.org/ontology#DE_000000025/head_and_neck_cancer_database/diagnosis")
    fatigue = URIRef("http://purl.roswellpark.org/ontology#DE_000000007/gu_medical_oncology_prostate_database/ae_fatigue/U")

    # create blank node for restriction
    restriction = BNode()

    # add axioms for restriction
    g = Graph()
    g.add((restriction, RDF.type, OWL.Restriction))
    g.add((restriction, OWL.onProperty, diagnosis))
    g.add((restriction, OWL.someValuesFrom, RDF.PlainLiteral))

    # create equivalence class
    g.add((fatigue, OWL.equivalentClass, restriction))
    """
    out = """
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
<{0}> owl:equivalentClass [ rdf:type owl:Restriction ;
                            owl:onProperty <{1}> ;
                            owl:someValuesFrom rdf:PlainLiteral
                          ] .
          """.format(URIRef(class_uri), URIRef(data_property_uri))
    return out


def nested_uri(*args):
    # pass in a series of portions of a URI to generate a nested URI with an instance after #
    out = str()
    i = 0
    while i <= len(args):
        if i == len(args)-1:
            out += '#%s'
        else:
            if i == 0:
                out += '%s'
            else:
                 out += '/%s'
        i += 1
        if i >= len(args):
            break

    return URIRef(out % args)

def sanitize_punctuation(to_convert):
    """
    :param to_convert: String to strip punctuation from
    :return: String with all punctuation replaced with a space
    """
    pattern = re.compile('[%s]' % re.escape(string.punctuation))

    # strip non number alpha chars
    pattern_2 = re.compile('[^A-Za-z0-9]+')
    out = pattern.sub(' ', str(to_convert))
    out = pattern_2.sub(' ', str(out))
    return out
