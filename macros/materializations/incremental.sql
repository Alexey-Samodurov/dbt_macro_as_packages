{% macro dbt_greenplum_validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="scd1") -%}


  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'scd1', 'sat', 'pit', 'scd2'
  {%- endset %}
  {% if strategy not in ['scd1', 'sat', 'pit', 'scd2'] %}
    {% do adapter.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}
  {% do return(strategy) %}
{% endmacro %}

{% macro _validate_partition_option(strategy, partition_column) %}
  {% if partition_column is not none and strategy not in ['scd1', 'sat','scd2']-%} 
    {% do adapter.raise_compiler_error("ERROR: partition is not supported for strategy: " ~ strategy ~".") %}
  {%- endif -%}
{% endmacro %}


{% macro dbt_greenplum_make_incremental_temp_table(strategy, sql) %}

  {% set tmp_relation = make_temp_relation(this) %}
  {% if strategy in ['scd1', 'sat', 'scd2'] %}
    {% do run_query(greenplum__create_table_as(True, tmp_relation, sql)) %}
  {% else %}
    {% set tmp_relation = sql %}
  {% endif %}
  {% do return(tmp_relation) %}
{% endmacro %}

{% macro dbt_greenplum_get_incremental_sql(strategy, tmp_relation_or_sql, target_relation) %}

  {% if strategy == 'scd1' %}
    {% do return(greenplum__get_scd1_sql(target_relation, tmp_relation_or_sql)) %}
  {% elif strategy == 'sat' %}
    {% do return(greenplum__get_sat_sql(target_relation, tmp_relation_or_sql)) %}
  {% elif strategy == 'pit' %}
    {% do return(greenplum__get_pit_sql(target_relation, tmp_relation_or_sql)) %}
  {% elif strategy == 'scd2' %}
    {% do return(greenplum__get_scd2_sql(target_relation, tmp_relation_or_sql)) %}  
  {% else %}
    {% do adapter.raise_compiler_error('invalid strategy: ' ~ strategy) %}    
  {% endif %}
{% endmacro %}

{% macro greenplum__get_main_part_delimiter() %}
  {# This macro returns a string which separate main sql chunk from temp tables in incremental loads #}
  {% do return("-- separate the wheat from the chaff") %}
{% endmacro %}

{% macro greenplum__get_fields_with_type_string(tmp_relation) %}
  {%- set gp_fields = get_columns_in_relation(tmp_relation) -%}
  {%- set fields_string -%} 
    {%- for field in gp_fields -%}
      {{ field.name }} {{ field.data_type }}{%- if not loop.last %},{{ SPACE }}{% endif %}
    {%- endfor -%}
  {%- endset -%}
  {% do return(fields_string) %}
{% endmacro %}  

{% materialization incremental, adapter='greenplum' -%}
  {% set target_relation = this %}
  {% set start_time = modules.datetime.datetime.utcnow() %}
  {% set existing_relation = load_relation(this) %}
  {%- set partition_column = greenplum__get_config_colnames('partition_column', config, replace_value=none) -%}
  {%- set partition_granularity = config.get('partition_granularity', 'days') -%}
  {%- set current_date_prolongation = config.get('current_date_prolongation', none) -%}
  
  {% set pd_fields = greenplum__get_config_colnames('pd_fields', config, replace_value='') %}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_greenplum_validate_get_incremental_strategy(config) -%}
  {%- set add_valid_to_dttm = True if strategy == 'scd2'  else False -%}

  {% do _validate_partition_option(strategy, partition_column) %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- Parse input sql and get final element --#}
  {#-- If no delimiter was found final_sql_chunk will contain full code of dbt model --#}
  {#-- And parsed_input_sql will be empty --#}
  {% set parsed_input_sql = sql.split(greenplum__get_main_part_delimiter())|list %}
  {% set final_sql_chunk = parsed_input_sql.pop() %}

  {#-- run sql code for tmp tables/queries if any code is provided --#}
  {% if parsed_input_sql[0]|length > 0 and 'select' in (parsed_input_sql[0]|lower) %}
    {% do run_query(parsed_input_sql[0]) %}
  {% endif %}
  
   {#-- Create temp relation if  strategy in ['scd1', 'sat', 'scd2'] else sql-guery--#}
  {% set tmp_relation_or_sql = dbt_greenplum_make_incremental_temp_table(strategy, final_sql_chunk) %}

  {#-- Create target relation in case it doesn't exist --#}
  {% if existing_relation is none or existing_relation.is_view %}
    {% do greenplum__create_schema(target_relation) %}
    {% if existing_relation.is_view %}
      {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
      {% do adapter.drop_relation(existing_relation) %}
    {% endif %} 
    {{ log("No existing relation, table " ~ target_relation ~ " will be created", info=True) }}
    {% if strategy == 'pit' %}
      {% do run_query(greenplum__create_pit_target_table(target_relation)) %}    
    {% else %}
      {% set fields_string = greenplum__get_fields_with_type_string(tmp_relation_or_sql) %}
      {% do run_query(greenplum__create_empty_table(False, target_relation, fields_string, add_sys_fields = True, partition_column = partition_column, add_valid_to_dttm = add_valid_to_dttm )) %} 
    {% endif %}

    {% do run_query(greenplum__grant_select_pd(target_relation, pd_fields)) %}

  {% endif %}
  {% set analyze_value = {} %}
  {% if partition_column -%} 
    {% set analyze_value = greenplum__prolong_range_partition(target_relation, tmp_relation_or_sql, partition_column, partition_granularity, current_date_prolongation = current_date_prolongation) %}
  {%- endif -%}  
  {% set build_sql = dbt_greenplum_get_incremental_sql(strategy, tmp_relation_or_sql, target_relation) %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}
  {% do run_query(greenplum__analyze_partitioned_relation_in_interval(target_relation, analyze_value)) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do adapter.commit() %}

  {% set target_relation = target_relation.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}
  
  {% set post_code = adapter.post_success_run_metric(this, start_time)  %}

  {% do adapter.send_eventbus(this)  %} 

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}