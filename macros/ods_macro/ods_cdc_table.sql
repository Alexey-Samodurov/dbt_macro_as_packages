{% macro greenplum__ods_cdc_insert(
    relations_map,
    constants_map,
    model_config
    ) %}

  {# -- Partition prolongation -- #}
  {% set analyze_value = {} %}
  {% if relations_map.cdc.partition.get('colnames', None) != None -%} 
        {% set analyze_value = greenplum__prolong_range_partition(
                                target_relation = relations_map.cdc.relation,
                                source_relation = relations_map.tmp.relation, 
                                partition_column = relations_map.cdc.partition.colnames, 
                                partition_granularity = relations_map.cdc.partition.granularity, 
                                current_date_prolongation = relations_map.cdc.partition.date_prolongation ) %}
  {%- endif -%}

  {# Find same columns in cdc_table and tmp_table. #}
  {% set same_columns_same_dtypes = greenplum__get_columns_same_name_same_dtypes(
                                      from_columns_array  = relations_map.tmp.columns, 
                                      to_columns_array    = relations_map.cdc.columns, 
                                      exclude_columns     = constants_map['TECH_FIELDS']) %}
  {% set columns_match_tmp  = same_columns_same_dtypes | map(attribute='name') | join(",") |string %}
  {% do relations_map.get('cdc').update({
            'columns_match_tmp' : columns_match_tmp
   }) %}

  {% if constants_map.macro_cdc_load != 'skip' %}
      {% set runnable_macro = context.get(constants_map.macro_cdc_load) %}
      {% set cdc_insert_query = runnable_macro(relations_map, constants_map, model_config) %}
      {% do run_query(cdc_insert_query) %}
  {% endif %}

  {# do run_query(greenplum__analyze_relation(relations_map.cdc.relation)) #}
  {% do run_query(greenplum__analyze_partitioned_relation_in_interval(relations_map.cdc.relation, analyze_value)) %}
{% endmacro %}


{# ================ Helping macro ================ #}

{% macro ods_cdc_simple_insert_query_es(relations_map, constants_map, model_config) %}
    {% set tmp_relation                    =   relations_map.tmp.relation %}
    {% set cdc_relation                    =   relations_map.cdc.relation %}
    {% set columns_match_tmp               =   relations_map.cdc.columns_match_tmp %}
    {% set dataflow_id                     =   constants_map['DATAFLOW_ID'] %}

    {% set cdc_insert_query %}
        insert into {{ cdc_relation }} ( {{ columns_match_tmp }}, dataflow_id, dataflow_dttm, ce_version, cdc_type_code, cdc_source_dttm, cdc_target_dttm )
        select 
            {{ columns_match_tmp }}
            , '{{ dataflow_id }}'
            , now()
            , ce_version
            , cdc_type_code
            , ce_time
            , cdc_target_dttm
        from {{ tmp_relation }}
    {% endset %}

    {{ return(cdc_insert_query) }}
{% endmacro %}

{% macro ods_cdc_simple_insert_query_jdbc_snap(relations_map, constants_map, model_config) %}
    {% set tmp_relation                    =   relations_map.tmp.relation %}
    {% set cdc_relation                    =   relations_map.cdc.relation %}
    {% set columns_match_tmp               =   relations_map.cdc.columns_match_tmp %}
    {% set dataflow_id                     =   constants_map['DATAFLOW_ID'] %}

    {% set cdc_insert_query %}
        insert into {{ cdc_relation }} ( {{ columns_match_tmp }}, dataflow_id, dataflow_dttm, cdc_type_code, cdc_source_dttm, cdc_target_dttm )
        select 
            {{ columns_match_tmp }}
            , '{{ dataflow_id }}'
            , now()
            , cdc_type_code
            , cdc_source_dttm
            , cdc_target_dttm
        from {{ tmp_relation }}
        where cdc_type_code is not null
            and prev_delete_flg != 1
    {% endset %}

    {{ return(cdc_insert_query) }}
{% endmacro %}

{% macro ods_cdc_simple_insert_query_msg_lib(relations_map, constants_map, model_config) %}
    {% set tmp_relation                    =   relations_map.tmp.relation %}
    {% set cdc_relation                    =   relations_map.cdc.relation %}
    {% set columns_match_tmp               =   relations_map.cdc.columns_match_tmp %}
    {% set dataflow_id                     =   constants_map['DATAFLOW_ID'] %}

    {% set cdc_insert_query %}
        insert into {{ cdc_relation }} ( {{ columns_match_tmp }}, dataflow_id, dataflow_dttm, cdc_type_code, cdc_source_dttm, cdc_target_dttm )
        select 
            {{ columns_match_tmp }}
            , '{{ dataflow_id }}'
            , now()
            , cdc_type_code
            , {{model_config['snapshot_time_field'] }}::timestamp(6)
            , now()::timestamp(6)
        from {{ tmp_relation }}
    {% endset %}

    {{ return(cdc_insert_query) }}
{% endmacro %}

{% macro ods_cdc_simple_insert_query_s3_incr(relations_map, constants_map, model_config )%}

    {% set tmp_relation         =   relations_map.tmp.relation %}
    {% set cdc_relation         =   relations_map.cdc.relation %}
    {% set array_columns        =   relations_map.cdc.columns_match_tmp %}
    {% set dataflow_id          =   constants_map['DATAFLOW_ID'] %}
    {% set version_field        =   model_config['version_field'] %}

    {% set cdc_insert_query %}
        insert into {{ cdc_relation }} ( {{ array_columns }}, dataflow_id, dataflow_dttm, cdc_type_code, cdc_source_dttm, cdc_target_dttm )
        select 
            {{ array_columns }}
            , '{{ dataflow_id }}'
            , now()
            , cdc_type_code
            , {{ version_field }}::timestamp(6)
            , now()::timestamp(6)
        from {{ tmp_relation }}
    {% endset %}

    {{ return(cdc_insert_query) }}
{% endmacro %}

{# Generate joins by load keys #}
{% macro greenplum__ods_generate_join_conditions(join_columns, left_alias, right_alias) %}
  {% set join_conditions_field = [] %}

  {% for join_key in join_columns.split(",") %}
    {% do join_conditions_field.append( ( left_alias ~ join_key ~ ' = ' ~ right_alias ~ join_key  ) ) %}
  {% endfor %}

  {{ return(join_conditions_field) }}
{% endmacro %}