import tkinter as tk
from tkinter import scrolledtext, messagebox
import sqlparse
import re

def analyze_sql(sql):
    """Analyzes a SQL statement and returns a list of result columns and their source tables/columns."""

    parsed = sqlparse.parse(sql)
    if not parsed:
        return "Error: Could not parse SQL statement."

    statement = parsed[0]
    tokens = statement.tokens

    with_clauses = {}
    select_parts = []
    from_parts = []
    join_parts = []
    where_parts = []

    # Extract WITH clauses
    with_clause_start = False
    with_clause_name = None
    with_clause_sql = ""
    for token in tokens:
        if token.is_keyword and token.value.upper() == "WITH":
            with_clause_start = True
        elif with_clause_start and token.is_identifier:
            with_clause_name = token.value
        elif with_clause_start and isinstance(token, sqlparse.sql.Parenthesis):
            with_clauses[with_clause_name] = str(token).strip("()")
            with_clause_start = False
        elif with_clause_start and token.value == ",":
            pass
        elif with_clause_start:
            pass
        elif token.is_keyword and token.value.upper() == "SELECT":
            select_parts.append(token)
        elif token.is_keyword and token.value.upper() == "FROM":
            from_parts.append(token)
        elif token.is_keyword and token.value.upper() == "JOIN":
            join_parts.append(token)
        elif token.is_keyword and token.value.upper() == "WHERE":
            where_parts.append(token)
        elif token.is_keyword and token.value.upper() == "LEFT":
            join_parts.append(token)
        elif token.is_keyword and token.value.upper() == "OUTER":
            join_parts.append(token)
        elif token.is_keyword and token.value.upper() == "ON":
            join_parts.append(token)

    # Extract SELECT columns
    select_columns = []
    select_part_str = "".join(str(part) for part in select_parts)
    select_part_str = re.sub(r'SELECT\s*', '', select_part_str, flags=re.IGNORECASE)
    select_part_str = re.sub(r'\s+FROM.*', '', select_part_str, flags=re.IGNORECASE)

    for item in select_part_str.split(","):
        select_columns.append(item.strip())

    # Extract FROM tables
    from_tables = []
    from_part_str = "".join(str(part) for part in from_parts)
    from_part_str = re.sub(r'FROM\s*', '', from_part_str, flags=re.IGNORECASE)
    from_part_str = re.sub(r'\s+LEFT\s+OUTER\s+JOIN.*', '', from_part_str, flags=re.IGNORECASE)
    from_part_str = from_part_str.split(" ")[0].strip()
    from_tables.append(from_part_str)

    # Extract JOIN tables
    join_tables = []
    join_part_str = "".join(str(part) for part in join_parts)
    join_part_str = re.sub(r'LEFT\s+OUTER\s+JOIN\s*', '', join_part_str, flags=re.IGNORECASE)
    join_part_str = re.sub(r'\s+ON.*', '', join_part_str, flags=re.IGNORECASE)
    join_part_list = join_part_str.split("JOIN")
    for join_table_item in join_part_list:
        join_table_item = join_table_item.strip()
        if join_table_item:
            join_tables.append(join_table_item.split(" ")[0].strip())

    source_mapping = {}
    for table_name, subquery in with_clauses.items():
        subquery_parsed = sqlparse.parse(subquery)[0]
        subquery_select_part = "".join(str(token) for token in subquery_parsed.tokens if token.is_keyword and token.value.upper() == "SELECT")
        subquery_from_part = "".join(str(token) for token in subquery_parsed.tokens if token.is_keyword and token.value.upper() == "FROM")

        subquery_select_part = re.sub(r'SELECT\s*', '', subquery_select_part, flags=re.IGNORECASE)
        subquery_select_part = re.sub(r'\s+FROM.*', '', subquery_select_part, flags=re.IGNORECASE)
        subquery_from_part = re.sub(r'FROM\s*', '', subquery_from_part, flags=re.IGNORECASE)
        subquery_from_part = subquery_from_part.split(" ")[0].strip()

        subquery_select_columns = [col.strip() for col in subquery_select_part.split(",")]
        for col in subquery_select_columns:
            source_mapping[table_name + "." + col] = (subquery_from_part, col)

    results = []
    for column in select_columns:
        column_parts = column.split(" AS ")
        result_column_name = column_parts[0].strip()
        if "." in result_column_name:
            table_alias, column_name = result_column_name.split(".")
            if table_alias in from_tables:
                results.append((column_name, from_tables[from_tables.index(table_alias)], column_name))
            elif table_alias in join_tables:
                results.append((column_name, join_tables[join_tables.index(table_alias)], column_name))
            elif table_alias in with_clauses:
                if table_alias + "." + column_name in source_mapping:
                    source_table, source_column = source_mapping[table_alias + "." + column_name]
                    results.append((column_name, source_table, source_column))

        else:
            if "*" in column:
                for table in from_tables + join_tables:
                    results.append(("All columns", table, "All columns"))
            else:
                if any(op in result_column_name for op in ["+", "-", "*", "/"]):
                    for table in from_tables + join_tables:
                        results.append((result_column_name.split(" ")[-1], table, result_column_name))
                else:
                    results.append((result_column_name, "Calculated", result_column_name))

    return results

def analyze_and_display():
    """Analyzes the SQL statement and displays the results in the output text box."""
    sql = sql_input.get("1.0", tk.END).strip()
    try:
        results = analyze_sql(sql)
        output_text.delete("1.0", tk.END)
        if isinstance(results, str):
            output_text.insert(tk.END, results)
        else:
            output_text.insert(tk.END, "RESULT COLUMN\tSOURCE TABLE\tSOURCE COLUMN\n")
            for result_column, source_table, source_column in results:
                output_text.insert(tk.END, f"{result_column}\t{source_table}\t{source_column}\n")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")

# UI setup
window = tk.Tk()
window.title("SQL Analyzer")

sql_label = tk.Label(window, text="Enter SQL Statement:")
sql_label.pack()

sql_input = scrolledtext.ScrolledText(window, width=80, height=10)
sql_input.pack()

analyze_button = tk.Button(window, text="Analyze SQL", command=analyze_and_display)
analyze_button.pack()

output_label = tk.Label(window, text="Analysis Results:")
output_label.pack()

output_text = scrolledtext.ScrolledText(window, width=80, height=10)
output_text.pack()

window.mainloop()