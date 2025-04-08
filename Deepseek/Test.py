import sqlglot
from sqlglot import exp

query = """
with tab1 as
(
  select a,b from db1.table1
)
,tab2 as
(
  select a from tab1
)
,tab3 as
(
  select
  t1.a
  ,t2.b
  from tab1 t1
  join tab2 t2
  on t1.a = t2.a
)
select
*
from tab3
"""

dependencies = {}

tree = sqlglot.parse_one(query)
joins = sqlglot.parse_one(query).find_all(exp.Join)

for cte in sqlglot.parse_one(query).find_all(exp.CTE):
  dependencies[cte.alias_or_name] = []

  cte_query = cte.this.sql()
  for table in sqlglot.parse_one(cte_query).find_all(exp.Table):
    dependencies[cte.alias_or_name].append(table.name)
print(dependencies)