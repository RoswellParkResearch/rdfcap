import os,sys,inspect

# this code is needed to referrence the util.uri_util module
# https://gist.github.com/JungeAlexander/6ce0a5213f3af56d7369
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
# import util.uri_util

# from uri_util import *
from util.uri_util import *

import rdflib
# from uri_util import *
from rdflib import RDFS, RDF, OWL, Namespace


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

    for subj, obj in g.subject_objects(RDFS.label):
        label = str(obj.encode('ascii', 'ignore')).lower().strip()
        uri_map[label] = str(subj.encode('ascii', 'ignore'))

    return uri_map


def make_field_uri_map(base_uri, field_names):
    """:returns a dictionary with k = field name and v = uri of field"""
    map = {}
    for field_name in field_names:
        map[field_name] = make_uri(base_uri, field_name)


def write_uri_map(uri_map, filename='uri_map.txt'):
    # save label2uri to file
    with open(filename, 'w') as f:
        f.write(str(uri_map)) # note: uri_map is converted to string


def load_uri_map(force=False, filename='uri_map.txt'):
    # create and the uri_map under the following two coditions:
    # the file does NOT exist OR force is True
    # uri_map_full_name = os.path.join(filepath, filename)
    uri_map_full_name = os.path.join(os.path.abspath('.'), filename)
    if force == True or os.path.exists(uri_map_full_name) == False:
        uri_map = make_uri_map()
        write_uri_map(uri_map, uri_map_full_name) # write uri_map to file
    # otherwise read uri_map from file
    else:
        # print "load from file"
        uri_map = eval(open(uri_map_full_name).read())

    # return uri_map
    return uri_map


def make_field_uri_map(base_uri, field_names):
    """
    given a base uri and a list of field names
    :return a dictionary of key = field name, value = uri of field
    """
    map = {}
    for field_name in field_names:
        field_name = parse_python_name(field_name) # remove invalid chars from field name
        map[field_name] = make_uri(base_uri, field_name)

    return map


def make_field_value_uri_map(base_uri, field_names):
    """
    given a base uri and a list of field names
    :return a dictionary of key = field name, value = uri of field
    """
    map = {}
    for field_name in field_names:
        map[field_name] = make_uri(base_uri, field_name)

    return map
# uri_map = make_uri_map('simple-dental-ontology.owl')
# write_uri_map(uri_map)
# print uri_map
# print uri_map.items()
# print pds.DataFrame(uri_map.items(), columns=['label', 'uri'])


