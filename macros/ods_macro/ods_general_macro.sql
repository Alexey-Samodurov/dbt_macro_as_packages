{% macro greenplum__ods_count_changes_from_meta(relation, meta_relation, fields_to_exclude=[]) %}

  {% set ddl_field = 'table_ddl_pxf' if relation.name.endswith("_ext") else 'table_ddl' %}
  {% call statement('get_count_changes', fetch_result=True, auto_begin=False) %}
    select 
      count(*) as changed_cols_cnt
    from (
      select
        table_schema, table_name,
        substring(colname_dtype, '^\S+') as column_name,
        substring(colname_dtype, '^\S+\s+(.*)') as data_type
      from grp_meta.ods_meta, unnest(string_to_array( {{ddl_field}} , ',')) as colname_dtype
      where table_schema = '{{ meta_relation.schema }}'
        and table_name = '{{ meta_relation.name }}'
    ) src
    full join (
      select 
        table_schema, 
        table_name, 
        column_name, 
        case
            when character_maximum_length is not null
                then data_type || '(' || character_maximum_length || ')'
            when datetime_precision is not null and data_type not in ('date', 'interval')
                then substring(data_type from 1 for position(' ' in data_type)-1 ) || '(' || datetime_precision || ')' || substring(data_type from position(' ' in data_type) )
                else data_type
        end as data_type
      from information_schema.columns
      where table_schema = '{{ relation.schema }}'
        and table_name = '{{ relation.name }}'
        {% if fields_to_exclude|length >= 1 %} and column_name not in ('{{fields_to_exclude|join("','")}}') {% else %} {% endif %}
    ) ext
      on lower(src.column_name) = lower(ext.column_name)
        and src.data_type = ext.data_type
    where src.table_schema is null
      or ext.table_schema is null
  {% endcall %}

  {{ return(load_result('get_count_changes').table.columns.changed_cols_cnt|first|int) }}
{% endmacro %}

{% macro greenplum__ods_fix_columns_dtypes(target_relation, target_meta_from_ods_meta, meta_of_existing_relation) %}

  {% set cols_with_different_dtypes_map = greenplum__get_columns_same_name_diff_dtypes(
                                            from_columns_array=target_meta_from_ods_meta, 
                                            to_columns_array=meta_of_existing_relation ) %}

  {% set allow_alt_columns = [] %}
  {% for next_row in cols_with_different_dtypes_map.items() %}

    {% set colname = next_row[0] %}
    {% set dtype_from = next_row[1]['from']|lower %}
    {% set dtype_to = next_row[1]['to']|lower %}

    {% if    (dtype_from == 'integer' and dtype_to == 'bigint') 
          or (dtype_from == 'timestamp(0) without time zone' and dtype_to == 'timestamp(6) without time zone') %}
      {% do allow_alt_columns.append('alter column '~colname~' type '~dtype_to ) %}
    {% elif (dtype_from == 'timestamp' and dtype_to == 'timestamp without time zone') or (dtype_from == 'timestamp without time zone' and dtype_to == 'timestamp') %}
      {# pass #}
    {% else %}
      {% do adapter.raise_compiler_error('ODS LOAD ERROR: ERROR DTYPES CONVERSION\n Table' ~ target_relation ~ ' column data type changed: ' ~ colname ~ ' from <' ~ dtype_from ~ '> to <' ~ dtype_to ~ '>.') %}
    {% endif %}
  {% endfor %}

  {% if allow_alt_columns|length > 0 %}

    {% set alter_dtype_query %}
      alter table {{ target_relation }} {{ allow_alt_columns|join(', ') }}
    {% endset %}

    {% do run_query(alter_dtype_query) %}
  {% endif %}

  {{ return(allow_alt_columns|length) }}
{% endmacro %}

{% macro greenplum__ods_add_missing_columns(target_relation, target_meta_from_ods_meta_columns, meta_of_existing_relation_columns) %}
  {% set missing_columns = greenplum__get_columns_diff_by_name(
                                      from_columns_array  = target_meta_from_ods_meta_columns, 
                                      to_columns_array    = meta_of_existing_relation_columns ) %}
  {% if missing_columns|length > 0 %}
    {% do greenplum__add_columns(target_relation, missing_columns) %}
  {% endif %}

  {{ return(missing_columns|length) }}
{% endmacro %}

{% macro greenplum__create_pxf_connection_string(
    foreign_path, 
    src_type, 
    src_connection_url, 
    src_db_name, 
    user,
    quote_columns=True  
)
%}
  {% set pxf_suffix = "pxf://" %}
  {% if src_type == 'postgres' %}
        {% set connection_string = 'PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=' ~ src_connection_url ~ '/' ~ src_db_name ~ "&" ~ user ~ "&QUOTE_COLUMNS=" ~ quote_columns %}
  {% elif src_type == '1c_bi' %}
        {% set connection_string = 'PROFILE=Jdbc&'~user %}
  {% endif %}

  {{ return(pxf_suffix ~ foreign_path ~ "?" ~ connection_string) }}
{% endmacro %}

{% macro get_custom_hook_decode_map(
    relations_map
)
%}
  {% set decode_map = {
      '{temp_table}':relations_map.tmp.relation|string, 
      '{cdc_table}':relations_map.cdc.relation|string,
      '{snp_table}':relations_map.snp.relation|string
  } %}
  {{ return(decode_map) }}
{% endmacro %}

{% macro run_custom_hooks(
    custom_hook, 
    decode_map
)
%}
  {% set ns = namespace(modified_item='') %}
  {% set custom_hooks = [] %}

  {% for item in custom_hook %}
    {% set ns.modified_item = item %}
    {% for key, value in decode_map.items() %}
      {% set ns.modified_item=ns.modified_item.replace(key, value) %}
    {% endfor %}
    {% set current_hook={"sql": ns.modified_item, "transaction": True} %}
    {% do custom_hooks.append(current_hook) %}
  {% endfor %}
  {{ run_hooks(custom_hooks, inside_transaction=True) }}
{% endmacro %}

{% macro set_cdc_table_cut_param(relations_map, model_config) %}
  {% do set_cut_param(
    relation=relations_map.tmp.relation, 
    param_name=model_config['cut_field'], 
    table_nm_relation=relations_map.cdc.relation) %}
{% endmacro %}

{% macro set_src_table_cut_param(relations_map, model_config) %}
  {% do set_cut_param(
    relation=relations_map.tmp.relation, 
    param_name=model_config['cut_field'], 
    table_nm_relation=relations_map.src.relation) %}
{% endmacro %}
