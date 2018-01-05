def dataframe_has_required_columns(df, payload):
    columns = ['time', 'src_host', 'src_port', 'dst_host', 'dst_port', 'protocol']
    if payload:
        columns.append("payload")
    return set(df.columns) == set(columns)
