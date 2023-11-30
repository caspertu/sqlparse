import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import pandas as pd
from collections import Counter


def is_select_statement(parsed_token):
    """ Check if the parsed token is a SELECT statement """
    return parsed_token.is_group and any(
        item.ttype is DML and item.value.upper() == 'SELECT' for item in parsed_token.tokens
    )


def extract_table_tokens(parsed_sql):
    """ Yield table tokens from a parsed SQL statement """
    from_clause_found = False
    for token in parsed_sql.tokens:
        if from_clause_found:
            if is_select_statement(token):
                yield from extract_table_tokens(token)
            elif token.ttype is Keyword:
                break
            else:
                yield token
        elif token.ttype is Keyword and token.value.upper() == 'FROM':
            from_clause_found = True


def extract_table_names(token_stream):
    """ Extract table names from a stream of SQL tokens """
    for token in token_stream:
        if isinstance(token, IdentifierList):
            yield from (identifier.get_real_name() for identifier in token.get_identifiers())
        elif isinstance(token, Identifier):
            yield token.get_real_name()


def parse_sql_tables(sql_query):
    """ Parse SQL query and extract table names """
    parsed_sql = sqlparse.parse(sql_query)[0]
    table_tokens = extract_table_tokens(parsed_sql)
    return list(extract_table_names(table_tokens))


def load_excel_data(file_path):
    """ Load Excel data from the given file path """
    return pd.read_excel(file_path)


def main():
    file_path = input("Please input file name:")
    try:
        excel_data = load_excel_data(file_path)
    except FileNotFoundError:
        print("File not exist, please check file name. File name need to be in format of 'xxx.xlsx'")
        return

    sql_queries = excel_data['SQL'].tolist()

    all_tables = []
    for sql in sql_queries:
        tables_in_query = parse_sql_tables(sql)
        all_tables.extend(tables_in_query)

    table_counts = Counter(all_tables)
    for table_name, count in table_counts.items():
        print(f"Table: {table_name}, Access Count: {count}")


if __name__ == '__main__':
    main()
