import sqlparse

def extract_table_and_partitions(query):
    parsed = sqlparse.parse(query)[0]
    tokens = [t for t in parsed.tokens if not t.is_whitespace]
    table_name = None
    for idx, token in enumerate(tokens):
        if token.ttype is None and token.value.upper() == "FROM" and idx + 1 < len(tokens):
            table_name = tokens[idx + 1].value
            break
    return table_name, [0, 1, 2, 3]  # simulate partition values
