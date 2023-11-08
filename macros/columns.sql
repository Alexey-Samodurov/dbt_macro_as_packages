{# adapter-specific implementation of get_columns_in_relation. Return Column.py objects with quotted names #}
{% macro greenplum__sql_convert_columns_in_relation(table) -%}
  {% set columns = [] %}
  {% for row in table %}
    {% do columns.append(api.Column(*row).get_instance_with_quotated_name()) %}
  {% endfor %}
  {{ return(columns) }}
{% endmacro %}