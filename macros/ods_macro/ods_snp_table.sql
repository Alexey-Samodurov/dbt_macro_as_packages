{% macro greenplum__ods_snp_insert(
    constants_map,
    relations_map,
    model_config
)
%}

    {# -- Partition prolongation #}
    {% set analyze_value = {} %}
    {% if relations_map.snp.partition.get('colnames', None) != None -%} 
        {% set analyze_value = greenplum__prolong_range_partition(
                                target_relation = relations_map.snp.relation,
                                source_relation = relations_map.tmp.relation, 
                                partition_column = relations_map.snp.partition.colnames, 
                                partition_granularity = relations_map.snp.partition.granularity, 
                                current_date_prolongation = relations_map.snp.partition.date_prolongation ) %}
    {%- endif -%}

    {# /* Include ES-specific tech fields (in 'reject' clause) */ #}
    {# /* No better way to manage this problem yet */ #}
    {% set snp_tech_fields_string = [] %}
    {%- for field in relations_map.snp.tech_fields  -%}
        {% do snp_tech_fields_string.append(field.name) %}
    {%- endfor -%}

    {% set same_columns = greenplum__get_columns_same_by_name(
                        from_columns_array  =   relations_map.tmp.columns, 
                        to_columns_array    =   relations_map.snp.columns,
                        exclude_columns     =   constants_map['TECH_FIELDS']|reject('in', snp_tech_fields_string)|list) %}
    {% set columns_match_tmp = same_columns | map(attribute='name') | join(",") |string %}
    {% do relations_map.get('snp').update({
            'columns_match_tmp' : columns_match_tmp
    }) %}
    
    {% if constants_map.macro_snp_load != 'skip' %}
        {% set runnable_macro = context.get(constants_map.macro_snp_load) %}
        {% do runnable_macro(
            relations_map,
            constants_map,
            model_config
        ) %}
    {% endif %}

    {# do run_query(greenplum__analyze_relation(relations_map.snp.relation)) #}
    {% do run_query(greenplum__analyze_partitioned_relation_in_interval(relations_map.snp.relation, analyze_value)) %}
{% endmacro %}

{# ================ helping macro ================ #}



{% macro _ods_create_incr_records_to_insert_view(view_to_create, src_table, order_field_desc) %}
    {% set create_incr_records_to_insert_view_query %}
        create view {{ view_to_create }} as 
        select * 
        from {{src_table }} t1
        where t1.{{ order_field_desc }} = 1
            and t1.cdc_type_code in ('I','U','D')
    {% endset %}
    {% do run_query(create_incr_records_to_insert_view_query) %}
{% endmacro %}

{% macro _ods_delete_from_target_by_key(target_relation, keys_to_delete_relation, order_field_desc, load_key) %}

    {% set join_clause = greenplum__ods_generate_join_conditions(load_key, 't2.', 't1.') %}
    {% set delete_from_target_query %}
        delete from {{ target_relation }} as t2
        using {{ keys_to_delete_relation }} as t1
        where {{ join_clause|join(" and ") }}
            and t1.{{ order_field_desc }} = 1
            and t1.cdc_type_code in ('D','U')
    {% endset %}
    {% do run_query(delete_from_target_query) %}
{% endmacro %}

{% macro ods_snp_prepare_tmp_and_insert_into_target(relations_map, constants_map, model_config) %}
    {% do _ods_delete_from_target_by_key(
        target_relation         =   relations_map.snp.relation, 
        keys_to_delete_relation =   relations_map.tmp.relation, 
        order_field_desc        =   constants_map['DESC_ROW_NUMERATION_FIELD'], 
        load_key                =   model_config['load_key']
    ) %}

    {% set incr_records_to_insert = greenplum__make_temp_relation(relations_map.base.relation, suffix='_insert_v') %}
    {% do _ods_create_incr_records_to_insert_view(
        view_to_create      = incr_records_to_insert, 
        src_table           = relations_map.tmp.relation,
        order_field_desc    = constants_map['DESC_ROW_NUMERATION_FIELD']
    ) %}
        
    {% do relations_map.get('tmp').update({
        'relation' : incr_records_to_insert
    })%}

    {% do run_query(_ods_insert_into_table(relations_map.snp.relation, relations_map.tmp.relation ,relations_map.snp.columns_match_tmp, constants_map['DATAFLOW_ID'])) %}
{% endmacro %}


{% macro _ods_insert_into_table(target_table, source_table, fields, dataflow_id) %}
    {% set insert_into_table_query %}
        insert into {{ target_table }} 
        ({{ fields }}, dataflow_id, dataflow_dttm)
        select
            {{ fields }}
            , '{{ dataflow_id }}'
            , now() 
        from {{source_table }}
    {% endset %}
    {% do return(insert_into_table_query) %}
{% endmacro %}


{% macro ods_snp_create_view_and_scd1(relations_map, constants_map, model_config) %}
    {% set snp_temp_view_relation = greenplum__ods_make_relation(base_relation = relations_map.tmp.relation, suffix = '_v') %}
    {% do greenplum__create_temp_view_as(snp_temp_view_relation, relations_map.tmp.relation, relations_map.snp.columns_match_tmp ) %}
    {% do run_query(greenplum__get_scd1_sql(relations_map.snp.relation, snp_temp_view_relation, constants_map['DATAFLOW_ID'], colname_delimiter = constants_map['colname_delimiter'])) %}
{% endmacro %}