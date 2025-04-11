import sqlglot
from sqlglot import exp
import tkinter as tk
from tkinter import ttk, scrolledtext

def process_sql(sql):
    try:
        main_query = sqlglot.parse_one(sql, read="snowflake")
    except Exception as e:
        return [{'result_column': 'Error', 'source_table': 'Error', 'source_column': str(e)}]

    # ctes = parsed.find_all(exp.CTE)
    # joins = parsed.find_all(exp.Join)
    # subqueris = parsed.find_all(exp.Subquery)

    cte_registry = {}
    if main_query.ctes:
        cte_registry = process_ctes(main_query)
    # if isinstance(parsed, exp.With):
    #     cte_registry = process_ctes(parsed)
    #     main_query = parsed.this
    # else:
    #     main_query = parsed

    result = []
    try:
        main_columns = process_query('*MAIN', main_query, cte_registry)
        for col_alias, sources in main_columns['columns'].items():
            for source_table, source_col in sources:
                result.append({
                    'result_column': col_alias.strip(),
                    'source_table': source_table,
                    'source_column': source_col
                })
    except Exception as e:
        result.append({'result_column': 'Error', 'source_table': 'Error', 'source_column': str(e)})
    
    return result

#------------ Working Process CTEs------
def process_ctes(parsed_ctes):
    cte_registry = {}
    for cte in parsed_ctes.find_all(exp.CTE):
        cte_name = cte.alias
        cte_query = cte.this
        processed_cte = process_query(cte.alias, cte_query, cte_registry)
        cte_registry[cte_name] = {
            'columns': processed_cte['columns'],
            'tables': processed_cte['tables']
        }
    return cte_registry

def process_query(query_alias, query, cte_registry):
    tables = []
    
    # Process FROM clause recursively
    if isinstance(query, exp.Select):
        from_clause = query.args.get("from")
        if from_clause:
            from_expressions = []
            
            # Handle main FROM expression
            if from_clause.this:
                from_expressions.append(from_clause.this)
            
 
            for expr in from_expressions:
                processed = process_from_expression(expr, cte_registry)
                if processed:
                    if isinstance(processed, list):
                        tables.extend(processed)
                    else:
                        tables.append(processed)

    # Handle JOIN expressions
    # from_expressions.extend(from_clause.expressions)
    join_clause = query.args.get("joins")
    if join_clause:
        process_join(join_clause, cte_registry)   

    # Process SELECT expressions with deep analysis
    select_columns = {}
    if isinstance(query, exp.Select):
        for expr in query.selects:
            alias = get_alias(expr)
            columns = []
            
            # Process all column references
            for column in expr.find_all(exp.Column):
                sources = trace_column_source(column, tables, cte_registry)
                if sources:
                    columns.extend(sources)
            
            # Handle calculated expressions
            if isinstance(expr, (exp.Mul, exp.Add, exp.Sub, exp.Div)):
                for arg in expr.find_all(exp.Column):
                    sources = trace_column_source(arg, tables, cte_registry)
                    if sources:
                        columns.extend(sources)
            
            select_columns[alias] = columns

    return {'columns': select_columns, 'tables': tables}

def process_from_expression(expr, cte_registry):
    if isinstance(expr, exp.Table):
        return process_table(expr)
    elif isinstance(expr, exp.Join):
        return process_join(expr, cte_registry)
    elif isinstance(expr, exp.Subquery):
        return process_subquery(expr, cte_registry)
    elif isinstance(expr, exp.Identifier):
        return process_cte_reference(expr, cte_registry)
    return None

def process_table(table_expr):
    return {
        'alias': table_expr.alias_or_name,
        'source_type': 'table',
        'source': table_expr.name,
        'columns': {}
    }

def process_join(join_expr, cte_registry):
    return [
        process_from_expression(join_expr.this, cte_registry),
        process_from_expression(join_expr.args.get('expression'), cte_registry)
    ]

def process_subquery(subq_expr, cte_registry):
    alias = subq_expr.aliasg
    processed = process_query(subq_expr.this, cte_registry)
    return {
        'alias': alias,
        'source_type': 'subquery',
        'source': processed,
        'columns': processed['columns']
    }

def process_cte_reference(ident_expr, cte_registry):
    cte_name = ident_expr.name
    alias = ident_expr.alias_or_name
    cte = cte_registry.get(cte_name, {'columns': {}, 'tables': []})
    return {
        'alias': alias,
        'source_type': 'cte',
        'source': cte,
        'columns': cte['columns']
    }

def trace_column_source(column, tables, cte_registry, visited=None):
    visited = visited or set()
    table_alias = column.table
    col_name = column.name
    sources = []
    
    target_alias = (table_alias or '').strip().lower()

    for table in tables:
        current_alias = (table.get('alias', '') or '').strip().lower()
        
        if current_alias != target_alias:
            continue

        if table['source_type'] == 'table':
            sources.append((table['source'], col_name))
        elif table['source_type'] in ('cte', 'subquery'):
            if (table['source'], col_name) in visited:
                continue
            visited.add((table['source'], col_name))
            
            # Recursive resolution for CTEs/subqueries
            if col_name in table['source']['columns']:
                for src_table, src_col in table['source']['columns'][col_name]:
                    if src_table == 'Error':
                        continue
                    # Trace through nested structures
                    sources.extend(
                        trace_column_source(
                            exp.Column(**{'this': src_col, 'table': None}),
                            table['source']['tables'],
                            cte_registry,
                            visited
                        )
                    )
        elif table['source_type'] == 'cte':
            cte = cte_registry.get(table['source']['source'], {})
            if col_name in cte.get('columns', {}):
                sources.extend(cte['columns'][col_name])

    return sources

def get_alias(expr):
    if isinstance(expr, exp.Alias):
        return expr.alias
    if isinstance(expr, exp.Column):
        return expr.name
    if isinstance(expr, (exp.Mul, exp.Add, exp.Sub, exp.Div)):
        return expr.sql()
    return expr.sql()

# GUI Implementation
def analyze_sql():
    sql_input = input_text.get("1.0", tk.END).strip()
    result = process_sql(sql_input)
    csv_output = "RESULT COLUMN,SOURCE TABLE,SOURCE COLUMN\n"
    for row in result:
        csv_output += f"{row['result_column']},{row['source_table']},{row['source_column']}\n"
    output_text.delete("1.0", tk.END)
    output_text.insert(tk.END, csv_output)

root = tk.Tk()
root.title("SQL Parser Analyzer")

input_label = ttk.Label(root, text="Enter SQL Statement:")
input_label.grid(row=0, column=0, padx=10, pady=5, sticky='w')

input_text = scrolledtext.ScrolledText(root, width=80, height=20)
input_text.grid(row=1, column=0, padx=10, pady=5)

analyze_button = ttk.Button(root, text="Analyze", command=analyze_sql)
analyze_button.grid(row=2, column=0, padx=10, pady=5)

output_label = ttk.Label(root, text="Result:")
output_label.grid(row=3, column=0, padx=10, pady=5, sticky='w')

output_text = scrolledtext.ScrolledText(root, width=80, height=20)
output_text.grid(row=4, column=0, padx=10, pady=5)

root.mainloop()