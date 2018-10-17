from src.ontology_class.rdfcap_base_generated_class import rdfcap_dev
from src.ontology_class.data_entity_ontology_generated_class import data_entity_ontology_dev
from src.generate_project_data import *
from src.convert_meta_eav import *
from src.util.translation_operations import *
from src.util.uri_util import *
from rdflib import Graph, RDF, RDFS, OWL, Namespace, BNode, URIRef, Literal, XSD
import pandas as pds
import sys

reload(sys)
sys.setdefaultencoding('utf8')

def get_processed_eav(df, project_meta):
    processed_data = []

    for row in df.itertuples():
        row = row.__dict__

        # composite record_id data
        pid = str(row['project_id']).replace('.0', '')
        event = str(row['event_id']).replace('.0', '')
        record = str(row['record']).replace('.0', '')
        instance = str(row['instance']) if str(row['instance']) != '(null)' else "0"
        record_id = '%s_%s_%s_%s' % (pid, event, record, instance)

        # value data
        val = str(row['value']).strip()
        field_name = str(row['field_name'])

        processed_data.append((record_id, field_name, sanitize_punctuation(val), pid, project_meta[pid]['project_name']))

    return pds.DataFrame([row for row in processed_data],
                         columns=['record_id', 'field_name', 'value', 'project_id', 'project_name'])


def translate_raw_eav(df, data_namespace_uri, project_data, meta_data, data_source="", data_source_base_uri="",
                      field_semantic_codes_file="", owl_file="", output_format="", output_dir=""):
    # ontology classes
    rdfcap = rdfcap_dev()
    dent = data_entity_ontology_dev()

    # build graph
    if len(owl_file.strip()) > 0:
        graph = Graph(identifier=data_namespace_uri)
        graph.parse(owl_file)
    else:
        graph = Graph(identifier=data_namespace_uri)

    # pass in a file with project_id, field_name, ncit in order to relate across DBs
    if field_semantic_codes_file != "":
        field_semantic_types = {}
        semantic_codes_df = pds.read_csv(field_semantic_codes_file, sep='\t')
        for row in semantic_codes_df.itertuples():
            row = row.__dict__
            project_id = str(row['project_id'])
            field_name = str(row['field_name'])

            if project_id not in field_semantic_types:
                field_semantic_types[project_id] = {}
            if field_name not in field_semantic_types[project_id]:
                field_semantic_types[project_id][field_name] = str(row['semantic_field_type'])

    # generate project data and add to graph
    generate_project_data(project_data, graph, data_namespace_uri)

    # create project metadata dict
    project_meta = generate_project_metadata(project_data)

    metadata = pds.read_csv(meta_data, sep='\t')

    # get list of form names, projects, fields
    field_names = []
    form_names = []
    projects = []
    fields_to_forms = {}
    for row in metadata.itertuples():
        row = row.__dict__

        # something odd happened in the metadata df so handle it
        try:
            pid = int(row['project_id'])
        except ValueError:
            continue

        field_name = str(row['field_name'])
        form_name = str(row['form_name'])
        field_label = str(row['element_label'])
        pid = str(row['project_id']).replace('.0', '')

        if pid not in projects:
            projects.append(pid)
        if (pid, field_name, field_label, form_name) not in field_names:
            field_names.append((pid, field_name, field_label, form_name))
        if (pid, form_name) not in form_names:
            form_names.append((pid, form_name))

        # field to form map
        if pid not in fields_to_forms:
            fields_to_forms[pid] = {}
        if form_name not in fields_to_forms[pid]:
            fields_to_forms[pid][form_name] = []
        if field_name not in fields_to_forms[pid][form_name]:
            fields_to_forms[pid][form_name].append(field_name)

    # project record class structure
    for pid in projects:
        class_uri = nested_uri(make_instance_uri(rdfcap.REDCap_project_record_uri),
                               project_meta[pid]['project_name']
                              )
        rdfcap.declare_class(graph,
                             class_uri,
                             rdfcap.REDCap_project_record_uri,
                             label='%s project record' % project_meta[pid]['app_title']
                             )
    for pid, form in form_names:
        form_uri = nested_uri(make_instance_uri(rdfcap.REDCap_project_record_uri),
                              project_meta[pid]['project_name'],
                              form
                             )
        project_uri = nested_uri(make_instance_uri(rdfcap.REDCap_project_record_uri),
                                 project_meta[pid]['project_name']
                                 )
        rdfcap.declare_class(graph,
                             form_uri,
                             project_uri,
                             label='%s record' % str(form).replace('_', ' ')
                            )

    # create project/form/field data property structure under record_value
    for pid, dta in project_meta.items():
        if pid in projects:
            field_uri = nested_uri(make_instance_uri(dent.record_value_uri),
                                   project_meta[pid]['project_name']
                                  )
            rdfcap.declare_data_property(graph,
                                         field_uri,
                                         dent.record_value_uri,
                                         label='%s record value' % project_meta[pid]['app_title']
                                         )
    for pid, form in form_names:
        if pid in projects:
            class_uri = nested_uri(make_instance_uri(rdfcap.REDCap_project_record_uri),
                                   project_meta[pid]['project_name'],
                                   form
                                  )
            field_uri = nested_uri(make_instance_uri(dent.record_value_uri),
                                   project_meta[pid]['project_name'],
                                   form
                                  )
            property_uri = nested_uri(make_instance_uri(dent.record_value_uri),
                                      project_meta[pid]['project_name']
                                     )
            rdfcap.declare_data_property(graph,
                                         field_uri,
                                         property_uri,
                                         label='%s record value' % str(form).replace('_', ' ')
                                         )
            graph.parse(data=equivalence_axiom(class_uri, field_uri), format='turtle')
    for (pid, fname, label, form) in field_names:
        if pid in projects:
            field_uri = nested_uri(make_instance_uri(dent.record_value_uri),
                                   project_meta[pid]['project_name'],
                                   form,
                                   fname
                                   )
            form_uri = nested_uri(make_instance_uri(dent.record_value_uri),
                                  project_meta[pid]['project_name'],
                                  form
                                 )
            rdfcap.declare_data_property(graph,
                                         field_uri,
                                         form_uri,
                                         label=str(label).strip()
                                         )

    # output file with classes, data properties, projects without individual data
    if output_format == "multi":
        out_structure = '%s/RDFCap_Structure.owl' % output_dir
        with open(out_structure, "w") as f:
            f.write(graph.serialize(format='xml'))
        f.close()
        # rebuild graph
        if len(owl_file.strip()) > 0:
            graph = Graph(identifier=data_namespace_uri)
            graph.parse(owl_file)
        else:
            graph = Graph(identifier=data_namespace_uri)

    # record/data generation
    processed_data = get_processed_eav(df, project_meta)

    created = []
    # declare records and data values for each
    for row in processed_data.itertuples():
        row = row.__dict__

        record_id = str(row['record_id']).strip()
        record_uri = nested_uri(make_instance_uri(rdfcap.REDCap_project_record_uri),
                                project_meta[row['project_id']]['project_name'],
                                record_id
                                )

        v = str(row['value'])
        field = str(row['field_name'])
        form_name = ""
        for form, fields in fields_to_forms[row['project_id']].items():
            if field in fields:
                form_name = form
                break

        if record_id not in created:
            project_uri = nested_uri(make_instance_uri(rdfcap.REDCap_project_record_uri),
                                     project_meta[row['project_id']]['project_name']
                                    )
            project_instance_uri = nested_uri(make_instance_uri(rdfcap.REDCap_project_uri),
                                              project_meta[row['project_id']]['project_name']
                                             )
            rdfcap.declare_individual(graph,
                                      record_uri,
                                      project_uri,
                                      label=record_id
                                      )
            rdfcap.record(graph,
                          record_uri,
                          str(row['record_id']).split('_')[2]
                          )
            rdfcap.instance(graph,
                            record_uri,
                            str(row['record_id']).split('_')[3]
                            )
            dent.member_of(graph,
                           record_uri,
                           project_instance_uri
                           )
            rdfcap.project_id(graph,
                              record_uri,
                              str(row['project_id'])
                              )
            rdfcap.project_name(graph,
                                record_uri,
                                str(row['project_name'])
                                )

            created.append(record_id)

        field_uri = nested_uri(make_instance_uri(dent.record_value_uri),
                               project_meta[row['project_id']]['project_name'],
                               form_name,
                               field
                              )
        graph.add((record_uri, field_uri, Literal(v)))

    return graph

if __name__ == "__main__":
    raw_data = r'/Volumes/cdn$/Projects/Phil Whalen/RDFCap/Raw Data/67 all data.txt'
    raw_df = pds.read_csv(raw_data, sep='\t')

    graph = translate_raw_eav(raw_df, # raw data
                              'http://purl.roswellpark.org/ontology/rdfcap', # namespace uri
                               r'/Volumes/cdn$/Projects/Phil Whalen/RDFCap/Raw Data/LST_RC Projects.txt', # project data
                               r'/Volumes/PH37399/processed_225 135 67 133 metadata.txt',
                               owl_file=r'/Volumes/cdn$/Projects/Phil Whalen/RDFCap/rdfcap-merged.owl',
                               output_format='multi',
                               output_dir='/Volumes/PH37399'
                              ) # redcap metadata

    print graph.serialize(format='turtle')

    with open('/Volumes/PH37399/GYN Clinical DB.owl', 'w') as f:
        f.write(graph.serialize(format='xml'))
    f.close()