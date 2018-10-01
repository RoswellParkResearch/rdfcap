import pandas as pds

def convert_meta_eav(metadata):
    # convert metadata into a more pleasant viewing/processing experience
    meta_df = pds.read_csv(metadata, sep='\t')

    proc = []
    for row in meta_df.itertuples():
        row = row.__dict__

        pid = str(row['project_id']).replace('.0', '')
        label = str(row['element_label'])
        field_name = str(row['field_name'])
        form_name = str(row['form_name'])
        type = str(row['element_type'])
        order = str(row['field_order'])
        enum_value = str()
        enum_label = str()

        enum = str(row['element_enum'])
        if enum != "(null)" and type != "calc":
            enum = enum.split('\\n')
            for value in enum:
                values = value.split(',')
                if len(values) > 1:
                    enum_value = values[0]
                    enum_label = str()
                    for idx, val in enumerate(values):
                        if idx > 0:
                            enum_label += val
                proc.append((pid, form_name, label, field_name, enum_value, enum_label, type, order))
        else:
            proc.append((pid, form_name, label, field_name, enum_value, enum_label, type, order))

    return pds.DataFrame([row for row in proc],
                         columns=['project_id', 'form_name', 'element_label', 'field_name', 'enum_value', 'enum_label',
                                  'element_type', 'field_order'])

if __name__ == "__main__":
    meta_data = r'Z:\Projects\Phil Whalen\RDFCap\Raw Data\135 and 225 meta.txt'
    meta_df = convert_meta_eav(meta_data)
    meta_df.to_csv('W:/processed_metadata.txt', sep='\t')