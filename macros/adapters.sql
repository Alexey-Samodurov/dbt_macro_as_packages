{% macro distributed_by(raw_distributed_by) %}
    {%- if raw_distributed_by is none -%}
      {{ return('') }}
    {%- endif -%}

    {%- if raw_distributed_by.strip('"')|lower == 'randomly' -%}
      {% set distributed_by_clause %}
        distributed randomly
      {%- endset -%}
    {%- else -%}
      {% set distributed_by_clause %}
        distributed by ({{ raw_distributed_by }})
      {%- endset -%}
    {%- endif -%}

    {{ return(distributed_by_clause) }}
{%- endmacro -%}

{% macro table_storage_parameters(raw_table_storage_parameter) %}
  {%- if raw_table_storage_parameter is none -%}
    {{ return('') }}
  {%- endif -%}

  {% set table_storage_parameters_clause %}
  with (
  {% if raw_table_storage_parameter is string -%}
    {% set table_storage_parameters_clause %}
       {{ raw_table_storage_parameter }}
    {%- endset -%}
  {%- else -%}
    {%- for param in raw_table_storage_parameter -%}
      {{ param }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  )
  {%- endset -%}

  {{ return(table_storage_parameters_clause) }}

{%- endmacro -%}

{% macro get_table_partition_parameters(partition_column) %}
  {%- if partition_column is none or partition_column == adapter.quote(None)-%}
    {{ return('') }}
  {%- endif -%}

  {% set table_partition_parameters_clause %}
  PARTITION BY RANGE({{ partition_column }}) 
           (DEFAULT PARTITION other)
  {%- endset -%}

  {{ return(table_partition_parameters_clause) }}

{%- endmacro -%}

{% macro greenplum__create_table_as(
  temporary,
  relation,
  sql,
  is_empty = false,
  add_sys_fields = false,
  custom_distributed_by='default'
) -%}
  {%- set unlogged = config.get('unlogged', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set raw_table_storage_parameter = config.get('table_storage_parameters', none) -%}

  {% if custom_distributed_by == 'default' %}
    {%- set raw_distributed_by = greenplum__get_config_colnames('distributed_by', config, replace_value='') -%}
  {% else %}
    {%- set raw_distributed_by = custom_distributed_by -%}
  {% endif %}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}
    temporary
  {%- elif unlogged -%}
    unlogged
  {%- endif %} table {{ relation }}
  {{ table_storage_parameters(raw_table_storage_parameter) }}

  as (
    {%if add_sys_fields -%}
    select null::text as dataflow_id,
           null::timestamp(0) as dataflow_dttm,
           foo.*
      from (
    {%- endif %}
    {{ sql }}
    {%if add_sys_fields -%}
      ) foo
    {%- endif %}
    {% if is_empty -%}
      limit 0
    {%- endif %}
  )
  {{ distributed_by(raw_distributed_by) }};
  {% if not temporary -%}
    alter table {{ relation }} owner to "{{ relation.owner }}";
  {%- endif %}

{%- endmacro %}

{% macro greenplum__create_empty_table(
  temporary,
  relation,
  fields,
  add_sys_fields = false,
  custom_distributed_by='default',
  partition_column = None,
  add_valid_to_dttm = false
) -%}
  {%- set unlogged = config.get('unlogged', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set raw_table_storage_parameter = config.get('table_storage_parameters', none) -%}
  
  {% if custom_distributed_by == 'default' %}
    {%- set raw_distributed_by = greenplum__get_config_colnames('distributed_by', config, replace_value='') -%}
  {% else %}
    {%- set raw_distributed_by = custom_distributed_by -%}
  {% endif %}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}
    temporary
  {%- elif unlogged -%}
    unlogged
  {%- endif %} table {{ relation }} (
  {%if add_sys_fields %}
    dataflow_id text,
    dataflow_dttm timestamp(0),
  {% endif %}
    {{ fields }}
  {%- if add_valid_to_dttm %}
    , valid_to_dttm timestamp(6) 
  {%- endif %}

  )
  {{ table_storage_parameters(raw_table_storage_parameter) }}
  {{ distributed_by(raw_distributed_by) }}
  {{ get_table_partition_parameters(partition_column) }}  ;
  {% if not temporary -%}
    alter table {{ relation }} owner to "{{ relation.owner }}";
  {%- endif %}
{%- endmacro %}

{% macro greenplum__create_external_table_as(relation, ext_relation_fields_string, connection_string) %}
  create external table {{ relation }} (
    {{ ext_relation_fields_string }}
  )
  location('{{ connection_string }}')
    on all
  format 'custom' (formatter = 'pxfwritable_import')
  encoding = 6;
  alter table {{ relation }} owner to "{{ relation.owner }}";
{% endmacro %}

{% macro greenplum__grant_select(relation, role_name) %}
    {% set grant_select_query %}
        grant select on {{ relation }} to {{ role_name }}
    {% endset %}
    {{ return(grant_select_query) }}
{% endmacro %}

{% macro greenplum__drop_table(relation, is_external=False, if_exists = False) %}
  {% set drop_query %}
    drop
      {% if is_external %} external {% endif %}
    table
    {% if if_exists %} if exists {% endif %}
      {{ relation }}
  {% endset %}

  {% do run_query(drop_query) %}
{% endmacro %}

{% macro greenplum__create_temp_view_as(temp_view_relation, src_relation, fields_to_include) %}
  {% set create_temp_view_query %}
    create temp view {{ temp_view_relation }} as
    select
      {{ fields_to_include }}
    from {{ src_relation }}
  {% endset %}
  {% do run_query(create_temp_view_query) %}
{% endmacro %}

{% macro greenplum__create_pit_target_table(relation) -%}
  {%- set raw_distributed_by = greenplum__get_config_colnames('distributed_by', config, replace_value=none) -%}
  {%- set raw_table_storage_parameter = config.get('table_storage_parameters', none) -%}
  {%- set pit_columns = config.get('pit_columns', none) -%}
  {%- set load_key = greenplum__get_config_colnames('load_key', config, replace_value=none) -%}

  {#- Check for required attributes for pit loader -#}
  {% if pit_columns is none %}
    {{ adapter.raise_compiler_error("For pit loader pit_columns config attribute is obligatory") }}
  {% endif %}
  {% if load_key is none %}
    {{ adapter.raise_compiler_error("For pit loader load_key config attribute is obligatory") }}
  {% endif %}

  create table {{ relation }}
  {{ table_storage_parameters(raw_table_storage_parameter) }}
  as (
    select null::text         as dataflow_id,
           null::timestamp(0) as dataflow_dttm,
           null::varchar(32)  as {{ load_key }},
           null::timestamp(6) as valid_from_dttm,
           null::timestamp(6) as valid_to_dttm,
           {%- for column in pit_columns %}
           null::timestamp(6) as {{ column }}
           {%- if not loop.last %},{% endif -%}
           {% endfor %}
  )
  {{ distributed_by(raw_distributed_by) }}
  ;
  alter table {{ relation }} owner to "{{ relation.owner }}";
{%- endmacro %}

{% macro greenplum__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set comma_separated_columns = ", ".join(index_config.columns) -%}
  {%- set index_name = index_config.render(relation) -%}

  create {% if index_config.unique -%}
    unique
  {%- endif %} index if not exists
  "{{ index_name }}"
  on {{ relation }} {% if index_config.type -%}
    using {{ index_config.type }}
  {%- endif %}
  ({{ comma_separated_columns }});
{%- endmacro %}

{% macro greenplum__create_schema(relation) -%}
  {% if relation.database -%}
    {{ adapter.verify_database(relation.database) }}
  {%- endif -%}

  {% call statement('get_schema_exists', fetch_result=True, auto_begin=False) %}
    select
      count(*) as schema_exists
    from pg_catalog.pg_namespace pn
    where nspname = '{{ relation.schema_nq }}';
  {% endcall %}

  {% set schema_exists= load_result('get_schema_exists').table.columns.schema_exists|first|int %}

  {% if not schema_exists -%}
    {%- call statement('create_schema') -%}
      {{ greenplum__create_roles_for_new_schema(relation ) }}
      create schema if not exists {{ relation.schema_nq }};
      alter schema {{ relation.schema_nq }} owner to {{ relation.owner }};
      grant usage on schema {{relation.schema_nq}} to {{relation.read_role_pd}};
      grant usage on schema {{relation.schema_nq}} to {{relation.read_role}};

    {%- endcall -%}
  {%- endif -%}
{% endmacro %}

{% macro greenplum__create_roles_for_new_schema(relation) -%}

  DO
  $do$
  BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles
        WHERE  rolname = '{{ relation.owner }}') THEN
        CREATE ROLE {{ relation.owner }} NOLOGIN  INHERIT;
    END IF;
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles
        WHERE  rolname = '{{ relation.read_role }}') THEN
        CREATE ROLE {{ relation.read_role }} NOLOGIN  INHERIT;
    END IF;
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles
        WHERE  rolname = '{{ relation.read_role_pd }}') THEN
        CREATE ROLE {{ relation.read_role_pd }} NOLOGIN  INHERIT;
    END IF;

    alter user {{ relation.read_role }} set default_transaction_read_only = true;
    {% if 'grp_ssa_' in relation.schema_nq -%}
      grant  {{ relation.read_role }} to all_ssa_r;
      grant  {{ relation.owner }} to all_ssa_w;
      grant  {{ relation.read_role_pd }} to all_ssa_r_pd;
    {%- endif %}
    {% if 'grp_ods_' in relation.schema_nq -%}
      grant  {{ relation.read_role }} to all_ods_r;
      grant  {{ relation.owner }} to all_ods_w;
      grant  {{ relation.read_role_pd }} to all_ods_r_pd;
    {%- endif %}
    {% if relation.schema_nq.startswith("test") -%}
      grant  {{ relation.read_role }} to all_tst_r;
      grant  {{ relation.owner }} to all_tst_w;
      grant  {{ relation.read_role_pd }} to all_tst_r_pd;
    {%- endif %}
  END
  $do$;

{% endmacro %}

{% macro greenplum__drop_schema(relation) -%}
  {% if relation.database -%}
    {{ adapter.verify_database(relation.database) }}
  {%- endif -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier().include(database=False) }} cascade;
    drop role if exists {{ relation.read_role_pd }};
    drop role if exists {{ relation.read_role }};
    drop role if exists {{ relation.owner }};
  {%- endcall -%}
{% endmacro %}

{% macro greenplum__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale,
          datetime_precision

      from {{ relation.information_schema('columns') }}
      where table_name = '{{ relation.identifier }}'
        {% if relation.schema %}
        and case when '{{ relation.schema }}' = 'pg_temp' and table_schema like 'pg_temp_%' then true
                               when table_schema = '{{ relation.schema }}' then true
                               else false
                               end
        {% endif %}
      order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(greenplum__sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro greenplum__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as database,
      tablename as name,
      schemaname as schema,
      'table' as type
    from pg_tables
    where schemaname ilike '{{ schema_relation.schema }}'
    union all
    select
      '{{ schema_relation.database }}' as database,
      viewname as name,
      schemaname as schema,
      'view' as type
    from pg_views
    where schemaname ilike '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro greenplum__information_schema_name(database) -%}
  {% if database_name -%}
    {{ adapter.verify_database(database_name) }}
  {%- endif -%}
  information_schema
{%- endmacro %}

{% macro greenplum__list_schemas(database) %}
  {% if database -%}
    {{ adapter.verify_database(database) }}
  {%- endif -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct nspname from pg_namespace
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro greenplum__check_schema_exists(information_schema, schema) -%}
  {% if information_schema.database -%}
    {{ adapter.verify_database(information_schema.database) }}
  {%- endif -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
    select count(*) from pg_namespace where nspname = '{{ schema }}'
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro greenplum__check_relation_exists_in_pg_table(pg_table, relation) %}
  {% set relation_string = relation.schema ~ "." ~ relation.name%}
  {% for r in pg_table.rows %}
    {% if r.relname|string == relation_string %}
    {% do return(True) %}
    {% endif %}
  {% endfor %}
  {% do return(False) %}
{% endmacro %}

{% macro greenplum__get_pg_table_info(relation_list) %}

  {% set where_list = [] %}
  {% for rel in relation_list%}
    {% do where_list.append("'" ~ rel.schema|string ~ "." ~ rel.name|string ~ "'") %}
  {% endfor %}

  {% call statement('pg_table_query', fetch_result=True, auto_begin=False) %}
    select distinct
      nspname || '.' || relname as relname
    from pg_class c
    left join pg_namespace n
      on n.oid = c.relnamespace
    where nspname || '.' || relname in ( {{ where_list|join(", ") }} )
  {% endcall %}

  {{ return(load_result('pg_table_query').table) }}
{% endmacro %}

{% macro greenplum__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro greenplum__snapshot_string_as_time(timestamp) -%}
    {%- set result = "'" ~ timestamp ~ "'::timestamp without time zone" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro greenplum__snapshot_get_time() -%}
  {{ current_timestamp() }}::timestamp without time zone
{%- endmacro %}

{#
  greenplum tables have a maximum length off 63 characters, anything longer is silently truncated.
  Temp relations add a lot of extra characters to the end of table namers to ensure uniqueness.
  To prevent this going over the character limit, the base_relation name is truncated to ensure
  that name + suffix + uniquestring is < 63 characters.
#}
{% macro greenplum__make_temp_relation(base_relation,suffix) %}
    {% set dt = modules.datetime.datetime.now() %}
    {% set dtstring = dt.strftime("%H%M%S%f") %}
    {% set suffix_length = suffix|length + dtstring|length %}
    {% set relation_max_name_length = 59 %}
    {% if suffix_length > relation_max_name_length %}
        {% do adapter.raise_compiler_error('Temp relation suffix is too long (' ~ suffix|length ~ ' characters). Maximum length is ' ~ (relation_max_name_length - dtstring|length) ~ ' characters.') %}
    {% endif %}
    {% set tmp_identifier = base_relation.identifier[:relation_max_name_length - suffix_length] ~ suffix ~ dtstring %}
    {% set tmp_schema = 'pg_temp' %}
    {% do return(base_relation.incorporate(
                                  path={
                                    "identifier": tmp_identifier,
                                    "schema": tmp_schema,
                                    "database": none
                                  })) -%}
{% endmacro %}

{% macro greenplum_escape_comment(comment) -%}
  {% if comment is not string %}
    {% do adapter.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  {%- set magic = '$dbt_comment_literal_block$' -%}
  {%- if magic in comment -%}
    {%- do adapter.raise_compiler_error('The string ' ~ magic ~ ' is not allowed in comments.') -%}
  {%- endif -%}
  {{ magic }}{{ comment }}{{ magic }}
{%- endmacro %}

{% macro greenplum__alter_relation_comment(relation, comment) %}
  {% set escaped_comment = greenplum_escape_comment(comment) %}
  comment on {{ relation.type }} {{ relation }} is {{ escaped_comment }};
{% endmacro %}

{% macro greenplum__alter_column_comment(relation, column_dict) %}
  {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
  {% for column_name in column_dict if (column_name in existing_columns) %}
    {% set comment = column_dict[column_name]['description'] %}
    {% set escaped_comment = greenplum_escape_comment(comment) %}
    comment on column {{ relation }}.{{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }} is {{ escaped_comment }};
  {% endfor %}
{% endmacro %}

{% macro greenplum__add_columns(relation, column_list ) %}
  {% if column_list %}

    {% set alter_table %}
      alter table {{relation}}
      {% for column in column_list  %}
        add column {{ column.name }} {{column.data_type}}{%- if not loop.last -%},{%- endif -%}
      {% endfor %}
    {% endset %}
    {% do run_query(alter_table ) %}
  {% else %}
    {{ logger(log_message="No columns to add" , log_level="INFO") }}
  {% endif %}
{% endmacro %}

{# Return Columns which are presented both in from_columns_array and to_columns_array by colname #}
{# Input  : from_columns_array - list of api.Column objects, to_columns_array - list of api.Column objects, exclude_columns - list of string #}
{# Return : list of api.Column objects #}
{% macro greenplum__get_columns_same_by_name(from_columns_array, to_columns_array, exclude_columns = []) %}
  {% set same_columns = [] %}
  {% for from_column in from_columns_array %}
    {% if from_column.name in to_columns_array | map(attribute='name') | list
        and from_column.name not in exclude_columns %}
      {% do same_columns.append(from_column) %}
    {% endif %}
  {% endfor %}
  {{ return(same_columns) }}
{% endmacro %}

{# Wrapper around greenplum__get_columns_same_by_name macro for relations usage #}
{# Input  : from_relation - relation, to_relation - relation, exclude_columns - list of string #}
{# Return : list of api.Column objects #}
{% macro greenplum__get_relations_columns_same_by_name(from_relation, to_relation, exclude_columns = []) %}
  {% set from_columns = greenplum__get_columns_in_relation(from_relation) %}
  {% set to_columns = greenplum__get_columns_in_relation(to_relation) %}
  {% set same_columns = greenplum__get_columns_same_by_name(from_columns, to_columns, exclude_columns) %}
  {{ return(same_columns) }}
{% endmacro %}

{# Return Columns which are presented in from_columns_array but not presented in to_columns_array by colname #}
{# Input  : from_columns_array - list of api.Column objects, to_columns_array - list of api.Column objects, exclude_columns - list of string #}
{# Return : list of api.Column objects #}
{% macro greenplum__get_columns_diff_by_name(from_columns_array, to_columns_array, exclude_columns = []) %}
  {% set columns_difference = [] %}
  {% for from_column in from_columns_array %}
    {% if from_column.name not in to_columns_array | map(attribute='name') | list
        and from_column.name not in exclude_columns
    %}
      {% do columns_difference.append(from_column) %}
    {% endif %}
  {% endfor %}
  {{ return(columns_difference) }}
{% endmacro %}

{# Return Columns which are presented in from_columns_array and also presented in to_columns_array by colname but have different dtypes #}
{# Input  : from_columns_array - list of api.Column objects, to_columns_array - list of api.Column objects, exclude_columns - list of string #}
{# Return : list of api.Column objects #}
{% macro greenplum__get_columns_same_name_diff_dtypes(from_columns_array, to_columns_array, exclude_columns = []) %}
  {% set columns_w_different_dtypes = {} %}
  {% for from_column in from_columns_array %}
    {% if from_column.name in to_columns_array | map(attribute='name') | list
        and from_column.name not in exclude_columns
    %}
      {% set to_columns_array_column_same_name = to_columns_array|selectattr("name", "equalto", from_column.name )|first %}
      {% if from_column.data_type|lower != to_columns_array_column_same_name.data_type|lower %}
        {% do columns_w_different_dtypes.update({
          from_column.name : {
            "from" : to_columns_array_column_same_name.data_type,
            "to" : from_column.data_type
          }
        }) %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {{ return(columns_w_different_dtypes) }}
{% endmacro %}

{# Return Columns which are presented in from_columns_array and also presented in to_columns_array by colname and have same dtypes #}
{# Input  : from_columns_array - list of api.Column objects, to_columns_array - list of api.Column objects, exclude_columns - list of string #}
{# Return : list of api.Column objects #}
{% macro greenplum__get_columns_same_name_same_dtypes(from_columns_array, to_columns_array, exclude_columns = []) %}
  {% set columns_w_same_dtypes = [] %}
  {% for from_column in from_columns_array %}
    {% if from_column.name in to_columns_array | map(attribute='name') | list
        and from_column.name not in exclude_columns
    %}
      {% set to_columns_array_column_same_name = to_columns_array|selectattr("name", "equalto", from_column.name )|first %}
      {% if from_column.data_type == to_columns_array_column_same_name.data_type %}
        {% do columns_w_same_dtypes.append(from_column) %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {{ return(columns_w_same_dtypes) }}
{% endmacro %}

{# Perform left join from from_columns_array to to_columns_array by colname. In case if any mismatches found, null Column appends to result. #}
{# Input  : from_columns_array - list of api.Column objects, to_columns_array - list of api.Column objects, exclude_columns - list of string #}
{# Return : list of api.Column objects #}
{% macro greenplum__get_columns_same_name_left_join(from_columns_array, to_columns_array, exclude_columns = []) %}
  {% set to_columns_colnames_lst = to_columns_array | map(attribute='name') | list %}
  {% set columns_same_name_left_join = [] %}
  {% for from_column in from_columns_array %}
    {% if from_column.name in to_columns_colnames_lst
        and from_column.name not in exclude_columns
    %}
      {% do columns_same_name_left_join.append(from_column) %}
    {% elif from_column.name not in to_columns_colnames_lst
        and from_column.name not in exclude_columns  %}
      {% do columns_same_name_left_join.append(api.Column("null", "")) %}
    {% endif %}
  {% endfor %}
  {{ return(columns_same_name_left_join) }}
{% endmacro %}

{% macro greenplum__quote_string_array(input_array) %}
  {% set output_array = [] %}

  {% if input_array is not sequence %}
    {% do adapter.raise_compiler_error("Error: input param is not an array.") %}
  {% endif %}

  {% if input_array|length|int > 0 %}
    {% for element in input_array %}
      {% if element is string and element != '' and element != None and element|lower != 'none' %}
        {% do output_array.append( adapter.quote(element) ) %}
      {% else %}
        {% do output_array.append( element ) %}
      {% endif %}
    {% endfor %}
  {% endif %}
  {{ return(output_array) }}
{% endmacro %}

{% macro greenplum__generate_quote(data_type_gp) %}
    {%- if  'date' in data_type_gp or 'time' in data_type_gp -%}
      {%- set quote_for_date = "'" -%}
    {%- else -%}
      {%- set quote_for_date = "" -%}
    {%- endif -%}
    {{ return(quote_for_date) }}
{% endmacro %}

{% macro greenplum__analyze_relation(relation) %}
    {% set analyze_query %}
        analyze {{relation}}
    {% endset %}
    {% do return(analyze_query) %}
{% endmacro %}

{% macro greenplum__get_non_pd_fields(relation, pd_fields=None) %}
    {% set fields = greenplum__get_columns_in_relation(relation) | map(attribute='name') | list %}
    {% set pd_fields_list = pd_fields.split(",")|list if pd_fields else [] %}
    {% set non_pd_fields_list = [] %}

    {% for field in fields %}
        {% if field not in pd_fields_list %}
            {% do non_pd_fields_list.append(field) %}
        {%- endif -%}
    {%- endfor -%}

    {% do return(non_pd_fields_list | join(', ')) %}
{%- endmacro -%}

{% macro greenplum__grant_select_pd(relation, pd_fields=None) %}
    grant select on {{ relation }} to {{ relation.read_role_pd }};
    {%- if pd_fields -%}
        grant select ({{ greenplum__get_non_pd_fields(relation, pd_fields) }}) on {{ relation }} to {{ relation.read_role }};
    {% else %}
        grant select on {{ relation }} to {{ relation.read_role }};
    {%- endif -%}
{%- endmacro -%}


{% macro greenplum__analyze_partitioned_relation_in_interval(relation, interval_dict = {} ) %}
    {% if interval_dict.min and interval_dict.max %} 
      {%- call statement('get_partition_nm_between_interval', fetch_result=True,auto_begin=false) -%}
        SELECT partitionschemaname||'.'||partitiontablename as partition_table
        FROM pg_catalog.pg_partitions
        WHERE schemaname = '{{relation.schema}}'
        AND  tablename = '{{relation.name}}'
        AND ((SUBSTRING(partitionrangeend FROM '''([^'']+)')::TIMESTAMP > '{{ interval_dict['min'] }}'::timestamp
	      AND SUBSTRING(partitionrangestart FROM '''([^'']+)')::TIMESTAMP <= '{{ interval_dict['max'] }}'::timestamp)
        OR partitionisdefault)
      {%- endcall -%}
      {% set partition_nm_between_interval = load_result('get_partition_nm_between_interval').table %}  
      {% set analyze_query %}
        {% for partition_table in partition_nm_between_interval.columns.partition_table %}
            analyze {{partition_table}} ;
        {% endfor %}
    {% endset %}
    {% else %}  
      {% set analyze_query %}
          analyze {{relation}} 
      {% endset %}
    {% endif %}         
    {% do return(analyze_query) %}
{% endmacro %}


{% macro greenplum__get_min_max_part_column_query(partition_column_nm, relation) %}
    {%- call statement('get_query_results', fetch_result=True,auto_begin=false) -%}
      select max({{ partition_column_nm}}),
      min({{ partition_column_nm}}),
      pg_typeof(min({{ partition_column_nm}}))
      from {{ relation}}
      where {{ partition_column_nm}} is not null
    {%- endcall -%}
    {% set sql_results= {} %}
    {%- if execute -%}
        {% set sql_results_table = load_result('get_query_results').table.columns %}
        {% for column_name, column in sql_results_table.items() %}
            {% do sql_results.update({column_name: column.values()|first}) %}
        {% endfor %}
    {%- endif -%}
    {% do return(sql_results) %}
{% endmacro %}

{# /* Provides single interface for parsing colname parameters presented as string from model files or dbt_project.yml */ #}
{# /* This macro should be used only in situation where values are colnames separated by comma */ #}
{# /* Macro remove whitespaces from value -> parse by comma -> add quotation -> join by comma and return result as string */ #}
{# /* Input: config_key -> config param name; config -> config map; replace_value -> default value if no key was found in config */ #}
{% macro greenplum__get_config_colnames(config_key, config, replace_value = '', separator = ',') %}

  {% do _validate_dict_not_empty(config) %}
  {% do _validate_non_empty_string(config_key) %}

  {% set config_value = config.get(config_key, replace_value) %}
  {% if config_value == '' or config_value is none or config_value|lower() == "none" %}
    {{ return(config_value) }}
  {% endif %}

  {% if config_value is not string %}
    {% do exceptions.raise_compiler_error("Param Read Error: get_config_colnames applied to non-string value. Key: " ~ config_key) %}
  {% endif %}

  {% set splitted_config_value = config_value.split(separator)|map("trim")|list %}
  {{ return(greenplum__quote_string_array(splitted_config_value)|join(separator)|string) }}
{% endmacro %}

{% macro _validate_non_empty_string(config_key) %}
  {% if execute %}
    {% if config_key is none or config_key == "" or config_key == "None" %}
        {% do exceptions.raise_compiler_error("Param Read Error: Config key should not be empty.") %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro _validate_dict_not_empty(config_to_use) %}
  {% if execute %}
    {% if not config_to_use %}
        {% do exceptions.raise_compiler_error("Param Read Error: No non-empty config has passed to macro.") %}
    {% endif %}
  {% endif %}
{% endmacro %}
