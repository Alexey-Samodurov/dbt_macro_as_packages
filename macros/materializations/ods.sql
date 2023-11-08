{% materialization ods, adapter='greenplum' -%}
    {% set start_time = modules.datetime.datetime.utcnow() %}

    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set ods_configs = greenplum__ods_model_configure( 
        model_database = this.database, 
        model_schema = this.schema, 
        model_name = this.identifier, 
        config = config
    ) %}
    {% set constants_map    = ods_configs['constants_map']  %}
    {% set relations_map    = ods_configs['relations_map']  %}
    {% set source_config    = ods_configs['source_config']  %}
    {% set model_config     = ods_configs['model_config']   %}
    {% set meta_table       = ods_configs['meta_table']     %}
    {% set post_tmp_hook = config.get("post_tmp_hook", None)%}

    {% do greenplum__ods_model_configure_db_elements(
        meta_table=meta_table,
        relations_map=relations_map,
        source_config=source_config,
        model_config=model_config,
        constants_map=constants_map
    ) %}

    {# ODS create and load tmp table #}
    {% do greenplum__ods_temp_table_insert( 
        relations_map, 
        model_config, 
        source_config,
        constants_map
    )%}

    {% if post_tmp_hook %}
        {% do run_custom_hooks(
            post_tmp_hook,
            get_custom_hook_decode_map(relations_map)
        )%}
    {% endif %}

    {% do greenplum__ods_cdc_insert(
        relations_map,
        constants_map,
        model_config
    ) %}

    {# Add deleted columns from target to temp table #}
    {% set miss_columns = greenplum__get_columns_diff_by_name(
        from_columns_array=relations_map.snp.columns, 
        to_columns_array=relations_map.tmp.columns,
        exclude_columns=constants_map['TECH_FIELDS']) %}
    {% do greenplum__add_columns(
        relations_map.tmp.relation, 
        miss_columns) %}
    {% do relations_map.get('tmp').update({
        'columns' : greenplum__get_columns_in_relation(relations_map.tmp.relation)
    }) %}

    {% do greenplum__ods_snp_insert(
        constants_map,
        relations_map,
        model_config
    ) %}

    {% if model_config['cut_field'] != None and model_config['cut_field'] != '' %}
        {% if constants_map.macro_set_cut_param != 'skip' %}
            {% set runnable_macro = context.get(constants_map.macro_set_cut_param) %}
            {% do runnable_macro( relations_map, model_config ) %}
        {% endif %}
    {% endif %}
    
    {% if config.get("add_udd_element_map", None) != None or config.get("add_udd_element", None) != None %}
        {% do greenplum__ods_fill_udd_dictionary(relations_map, source_config) %}
    {% endif %}

    {%- call statement('main') -%}
        {{ 'select 1' }}
    {%- endcall -%}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% do adapter.commit() %}

    {% set post_code = adapter.post_success_run_metric(this, start_time)  %}
    {% do adapter.send_eventbus(this)  %}

    {{ return({'relations': [relations_map.cdc.relation]}) }}

{% endmaterialization %}
