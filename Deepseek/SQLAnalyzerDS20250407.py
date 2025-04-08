import sqlglot
from sqlglot import exp
import tkinter as tk
from tkinter import ttk, scrolledtext

def process_sql(sql):
    try:
        parsed = sqlglot.parse_one(sql)
        
        # Initialize CTE registry
        cte_registry = {}

        # for cte in parsed.find_all(exp.CTE):
        #     cte_name = cte.alias
        #     cte_query = cte.this
        #     processed_cte = process_query(cte_query, cte_registry)
        #     cte_registry[cte_name] = processed_cte

        # main_query = parsed.this    
        
        #Process CTEs if present
        if isinstance(parsed, exp.With):
            for cte in parsed.expressions:
                cte_name = cte.alias
                cte_query = cte.this
                processed_cte = process_query(cte_query, cte_registry)
                cte_registry[cte_name] = processed_cte
            main_query = parsed.this
        else:
            main_query = parsed

        # Process main query
        result = []
        main_columns = process_query(main_query, cte_registry)
        for col_alias, sources in main_columns['columns'].items():
            for source_table, source_col in sources:
                result.append({
                    'result_column': col_alias.strip(),
                    'source_table': source_table,
                    'source_column': source_col
                })
        
        return result

    except Exception as e:
        return [{'result_column': 'Error', 'source_table': 'Error', 'source_column': str(e)}]

def process_query(query, cte_registry):
    tables = []
    
    # Process FROM clause
    if isinstance(query, exp.Select):
        from_clause = query.args.get("from")
        if from_clause:
            # Handle JOINs and tables
            for expr in from_clause.expressions:
                if isinstance(expr, exp.Table):
                    tables.append({
                        'alias': expr.alias_or_name,
                        'source_type': 'table',
                        'source': expr.name,
                        'columns': {}
                    })
                elif isinstance(expr, exp.Join):
                    left = expr.this
                    right = expr.args.get('expression')
                    if isinstance(left, exp.Table):
                        tables.append({
                            'alias': left.alias_or_name,
                            'source_type': 'table',
                            'source': left.name,
                            'columns': {}
                        })
                    if isinstance(right, exp.Table):
                        tables.append({
                            'alias': right.alias_or_name,
                            'source_type': 'table',
                            'source': right.name,
                            'columns': {}
                        })
                elif isinstance(expr, exp.Identifier) and expr.name in cte_registry:
                    tables.append({
                        'alias': expr.alias_or_name,
                        'source_type': 'cte',
                        'source': cte_registry[expr.name],
                        'columns': cte_registry[expr.name]['columns']
                    })

    # Process SELECT columns
    select_columns = {}
    if isinstance(query, exp.Select):
        for expr in query.selects:
            alias = expr.alias_or_name
            columns = []
            
            # Find all column references
            for column in expr.find_all(exp.Column):
                table_alias = column.table
                col_name = column.name
                
                for table in tables:
                    if table['alias'].lower() == (table_alias or '').lower():
                        if table['source_type'] == 'table':
                            columns.append((table['source'], col_name))
                        elif table['source_type'] == 'cte':
                            if col_name in table['source']['columns']:
                                columns.extend(table['source']['columns'][col_name])
            
            select_columns[alias] = columns

    return {'columns': select_columns, 'tables': tables}

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