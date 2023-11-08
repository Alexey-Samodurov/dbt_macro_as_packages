
{# ================================ Configure ods load params ================================ #}

{% macro greenplum__ods_model_configure(model_database, model_schema, model_name, config) %}

    {# ======== Precalculate vars ======== #}

    {% set NAME_SPLITTED = model_name.split("_") %}
    {% if NAME_SPLITTED[0] in  ['jdbc','jds3','es00','kfs3','sfs3','s3s3','aps3','sbs3','sds3'] %}
        {% set true_ods_name = NAME_SPLITTED[2:] | join('_') %}
        {% set true_ods_schema = NAME_SPLITTED[:2] | join('_') %}
    {% else %}
        {% set true_ods_name = NAME_SPLITTED[1:] | join('_') %}
        {% set true_ods_schema = NAME_SPLITTED[:1] | join('_') %}
    {% endif %}

    {% set base_relation = api.Relation.create(
                                database = model_database,
                                schema = model_schema,
                                identifier = true_ods_name ) %}

    {# ======== Parse and calculate ods configs ======== #}
    {% set source_config    = greenplum__ods_generate_source_config(config, true_ods_schema) %} 
    
    {% set constants_map    = greenplum__ods_generate_constants_map(config) %}
    {% set constants_map    = _ods_constants_map_set_specific_constants_by_source_type(source_config['is_src_external'], constants_map) %}

    {% set meta_table       = run_query(greenplum__ods_get_meta_table_query(base_relation, False)) %}

    {% set model_config     = greenplum__ods_generate_model_config(config) %}
    {% set constants_map    = _ods_constant_map_set_custom_tech_fields(constants_map, model_config)  %}
    {% set constants_map    = _ods_constant_map_set_load_strategy(constants_map, model_config)  %}

    {% set relations_map    = greenplum__ods_generate_relation_map(base_relation) %}
    {% set relations_map    = _ods_relation_map_set_src_relation(relations_map, source_config['src_db_short'], source_config['is_src_external']) %}
    {% set relations_map    = _ods_relation_map_set_trg_relations(relations_map) %}
   
    {# /* Define extra fields to be passed from tmp-table to cdc-table. */ #}
    {# /* Be sure to add addable column to tmp-table first. */ #}
    {# /* WARNING: Columns should be created with the same data it will be read from DB. See columns.py constructor. */ #}
    {% if constants_map.macro_set_cdc_tech_fields != 'skip' %}
        {% set runnable_macro = context.get(constants_map.macro_set_cdc_tech_fields) %}
        {% set relations_map =  runnable_macro(relations_map, model_config) %}
    {% endif %}
    {# /* Define extra fields to be passed from tmp-table to snp-table. */ #}
    {% if constants_map.macro_set_snp_tech_fields != 'skip' %}
        {% set runnable_macro = context.get(constants_map.macro_set_snp_tech_fields) %}
        {% set relations_map =  runnable_macro(relations_map, model_config) %}
    {% endif %}

    {% if constants_map.macro_set_tmp_table_fields != 'skip' %}
        {% set runnable_macro = context.get(constants_map.macro_set_tmp_table_fields) %}
        {% set relations_map =  runnable_macro(relations_map, model_config, meta_table) %}
    {% endif %}

    {# Check all prerequisites to be ok #}
    {% do greenplum__ods_validate_configs(
        relations_map,
        source_config,
        model_config,
        meta_table,
        constants_map
    ) %}

    {{ return({
        'constants_map' : constants_map, 
        'relations_map' : relations_map, 
        'source_config' : source_config, 
        'model_config'  : model_config,
        'meta_table'    : meta_table
    }) }}
{% endmacro %}

{% macro greenplum__ods_model_configure_meta_sync(model_database, model_schema, model_name, config) %}
    {# ======== Precalculate vars ======== #}

    {% set NAME_SPLITTED = model_name.split("_") %}
    {% if NAME_SPLITTED[0] in  ['jdbc','jds3','es00','kfs3','sfs3','s3s3','aps3','sbs3','sds3'] %}
        {% set true_ods_name = NAME_SPLITTED[2:] | join('_') %}
        {% set true_ods_schema = NAME_SPLITTED[:2] | join('_') %}
    {% else %}
        {% set true_ods_name = NAME_SPLITTED[1:] | join('_') %}
        {% set true_ods_schema = NAME_SPLITTED[:1] | join('_') %}
    {% endif %}

    {% set base_relation = api.Relation.create(
                                database = model_database,
                                schema = model_schema,
                                identifier = true_ods_name ) %}

    {# ======== Parse and calculate ods configs ======== #}
    {% set source_config    = greenplum__ods_generate_source_config(config, true_ods_schema) %}

    {% set constants_map    = greenplum__ods_generate_constants_map(config) %}

    {% set relations_map    = greenplum__ods_generate_relation_map(base_relation) %}
    {% set relations_map    = _ods_relation_map_set_src_relation(relations_map, source_config['src_db_short'], source_config['is_src_external']) %}

    {% set model_config     = greenplum__ods_generate_model_config(config) %}
    {% set constants_map    = _ods_constant_map_set_custom_tech_fields(constants_map, model_config)  %}

    {{ return({
        'constants_map' : constants_map, 
        'relations_map' : relations_map, 
        'source_config' : source_config, 
        'model_config'  : model_config
    }) }}
{% endmacro %}

{# ================================ Config generators ================================ #}

{% macro greenplum__ods_generate_constants_map(config) %}
    {% set dt = modules.datetime.datetime.now() %}  
    {% set constants_map = {
        'TECH_FIELDS'               :   greenplum__quote_string_array(
                                            ['dataflow_dttm', 'dataflow_id', 'hashdiff_key', 
                                            'cdc_source_dttm', 'cdc_transfer_dttm', 'cdc_target_dttm',
                                            'ce_type', 'ce_version', 'ce_time', 'snp_delete_flg'] ) ,
        'important_tech_fields_list':   config.get('important_tech_fields_list',[]),
        'DATAFLOW_ID'               :   'dbt__' ~ dt.strftime("%H%M%S%f"),
        'colname_delimiter'         :   '"'
    } %}
    {{ return(constants_map) }}
{% endmacro %}

{% macro _ods_constant_map_set_custom_tech_fields(constants_map, model_config) %}

    {# /* Custom tech fields keys can be specified here.*/ #}
    {% set CUSTOM_TECH_FIELDS_KEYS = [
        'cdc_type_field', 
        'version_field',
        'snapshot_time_field',
        'addition_tech_fields'
    ] %}

    {# /* Iterate over model_config keys and extract values with given keys. */ #}
    {% set custom_tech_fields = [] %}
    {% for k, v in model_config.items() %}
      {% if k in CUSTOM_TECH_FIELDS_KEYS and v != None %}
          {% do custom_tech_fields.extend(v.replace('"', '').split(",")) %} 
      {% endif %}
    {% endfor %}

    {# /* Save Custom values to contants_map['TECH_FIELDS'] and perform unique filter */ #}
    {% do constants_map['TECH_FIELDS'].extend(
                                        greenplum__quote_string_array(custom_tech_fields)) %}
    {% do constants_map.update({
        'TECH_FIELDS'   :   constants_map['TECH_FIELDS']|unique|list
    }) %}

    {{ return(constants_map) }}
{% endmacro %}

{% macro _ods_constants_map_set_specific_constants_by_source_type(is_src_external, constants_map) %}
    {% if not is_src_external %}
        {% do constants_map.update({
            'ASC_ROW_NUMERATION_FIELD'  :   'rn_asc',
            'DESC_ROW_NUMERATION_FIELD' :   'rn_desc',
        }) %}
    {% endif %}

    {{ return(constants_map) }}
{% endmacro %}

{% macro _ods_constant_map_set_load_strategy(constants_map, model_config) %}
    {# /* Manual dict with load strategy */ #}
    {% set load_strategy_depends_on_logical_src = {
        'es'          :   {'macro_set_cdc_tech_fields'       : '_ods_set_cdc_tech_fields_es',
                           'macro_set_snp_tech_fields'       : '_ods_set_snp_tech_fields_es',
                           'macro_set_tmp_table_fields'      : '_ods_set_tmp_table_fields_es',
                           'macro_filter_input_data'         : 'greenplum__ods_filter_input_data_type_es',
                           'macro_snp_load'                  : 'ods_snp_prepare_tmp_and_insert_into_target',
                           'macro_cdc_load'                  : 'ods_cdc_simple_insert_query_es',
                           'macro_additional_validate'       : '_ods_validate_add_es_tests',
                           'macro_set_cut_param'             : 'set_src_table_cut_param'
                           },                                
        'jdbc_snap'   :   {'macro_set_cdc_tech_fields'       : '_ods_set_cdc_tech_fields',
                           'macro_set_snp_tech_fields'       : '_ods_set_snp_tech_fields',
                           'macro_set_tmp_table_fields'      : '_ods_set_tmp_table_fields_jdbc_snap',
                           'macro_filter_input_data'         : 'greenplum__ods_filter_input_data_type_jdbc_snap',
                           'macro_snp_load'                  : 'ods_snp_create_view_and_scd1',
                           'macro_cdc_load'                  : 'ods_cdc_simple_insert_query_jdbc_snap',
                           'macro_additional_validate'       : 'skip',
                           'macro_set_cut_param'             : 'set_cdc_table_cut_param'
                          },                                 
        'message_lib' :   {'macro_set_cdc_tech_fields'       : '_ods_set_cdc_tech_fields',
                           'macro_set_snp_tech_fields'       : '_ods_set_snp_tech_fields_msg_lib',
                           'macro_set_tmp_table_fields'      : '_ods_set_tmp_table_fields_msg_lib',
                           'macro_filter_input_data'         : 'greenplum__ods_filter_input_data_type_msg_lib',
                           'macro_snp_load'                  : 'ods_snp_prepare_tmp_and_insert_into_target',
                           'macro_cdc_load'                  : 'ods_cdc_simple_insert_query_msg_lib',
                           'macro_additional_validate'       : '_ods_validate_add_msg_lib_tests',
                           'macro_set_cut_param'             : 'set_src_table_cut_param'
                          },
        's3_incr'     :   {'macro_set_cdc_tech_fields'       : '_ods_set_cdc_tech_fields',
                           'macro_set_snp_tech_fields'       : '_ods_set_snp_tech_fields_s3_incr',
                           'macro_set_tmp_table_fields'      : '_ods_set_tmp_table_fields_s3_incr',
                           'macro_filter_input_data'         : 'greenplum__ods_filter_input_data_type_s3_incr',
                           'macro_snp_load'                  : 'ods_snp_prepare_tmp_and_insert_into_target',
                           'macro_cdc_load'                  : 'ods_cdc_simple_insert_query_s3_incr',
                           'macro_additional_validate'       : '_ods_validate_add_s3_incr_tests',
                           'macro_set_cut_param'             : 'set_src_table_cut_param'
                          },                                  
        'custom'     :    {'macro_set_cdc_tech_fields'       : config.get("macro_set_cdc_tech_fields",     '_ods_set_cdc_tech_fields'),
                           'macro_set_snp_tech_fields'       : config.get("macro_set_snp_tech_fields",     'skip'),
                           'macro_set_tmp_table_fields'      : config.get("macro_set_tmp_table_fields",     '_ods_set_tmp_table_fields_custom_default'),
                           'macro_filter_input_data'         : config.get("macro_filter_input_data",     'skip'),
                           'macro_snp_load'                  : config.get("macro_snp_load",     'skip'),
                           'macro_cdc_load'                  : config.get("macro_cdc_load",     'skip'),
                           'macro_additional_validate'       : 'skip',
                           'macro_set_cut_param'             : config.get("macro_set_cut_param",     'skip') 
                          }                              
                                              } %}                                        
    {# /* Choose macro depend on load strategy */ #}
    {% do constants_map.update(
        load_strategy_depends_on_logical_src[model_config.type_load_strategy]
    ) %}  
    {{ return(constants_map) }}
{% endmacro %}

{% macro greenplum__ods_generate_relation_map(base_relation) %}

    {% set relations_map = { 
        'base' : {
            'relation' : base_relation,
            'columns'  : greenplum__ods_meta_get_table_ddl(base_relation)
        }
    }%}
    
    {{ return(relations_map) }}
{% endmacro %}

{% macro _ods_relation_map_set_trg_relations(relations_map) %}

    {% do relations_map.update({ 
        'tmp'      :   {
            'relation'  : greenplum__make_temp_relation(relations_map.base.relation, '_tmp') 
        },
        'cdc'      :   {
            'include'   : config.get("cdc_include", True),
            'partition' : {
                'colnames' : greenplum__get_config_colnames('cdc_partition_column', config, replace_value=''),
                'granularity' : config.get('cdc_partition_granularity', ''),
                'date_prolongation' : config.get("cdc_current_date_prolongation", False)
            } if greenplum__get_config_colnames('cdc_partition_column', config, replace_value='') != '' else {},
            'relation'  : greenplum__ods_make_relation(base_relation = relations_map.base.relation, suffix = '_cdc')
        } ,
        'snp'      :   {
            'include'   : config.get("snp_include", True),
            'partition' : {
                'colnames' : greenplum__get_config_colnames('snp_partition_column', config, replace_value=''),
                'granularity' : config.get('snp_partition_granularity', ''),
                'date_prolongation' : config.get("snp_current_date_prolongation", False)
            } if greenplum__get_config_colnames('snp_partition_column', config, replace_value='') != '' else {},
            'relation'  : greenplum__ods_make_relation(base_relation = relations_map.base.relation, suffix = '_snp')
        }
    }) %}
    {{ return(relations_map) }}
{% endmacro %}

{% macro _ods_relation_map_set_src_relation(relations_map, src_db_short, is_src_external) %}

    {% set EXTERNAL_TABLE_SUFFIX = '_ext' %}
    {% set INTERNAL_TABLE_PREFIX = src_db_short %}
    {% set INTERNAL_TABLE_SUFFIX = '_view' %}

    {% if is_src_external %}  
        {% do relations_map.update({
            'src' : {
                'relation' : greenplum__ods_make_relation(
                                        base_relation = relations_map.base.relation, 
                                        suffix = EXTERNAL_TABLE_SUFFIX)
                } 
        }) %}
    {% else %}
        {% do relations_map.update({
            'src' : {
                'relation' : greenplum__ods_make_relation(
                                        base_relation = relations_map.base.relation, 
                                        schema = "grp_stg_"~INTERNAL_TABLE_PREFIX, 
                                        suffix = INTERNAL_TABLE_SUFFIX)
            } 
        }) %}
    {% endif %}
    {{ return(relations_map) }}
{% endmacro %}

{% macro greenplum__ods_generate_source_config(config, src_db_short) %}
    {% set source_config = {
        'src_type'                  :   config.get("src_type",                      None) ,
        'src_db_short'              :   src_db_short,
        'src_schema_name'           :   config.get("src_schema_name",               None) ,
        'src_db_name'               :   config.get("src_db_name",                   None) ,
        'pxf_user'                  :   config.get("pxf_user",                      None) ,
        'connection_string'         :   greenplum__ods_choose_env_connection_url(config, target.name|string) 
    } %}
    {% set source_config = _ods_set_source_type(source_config) %}
    {{ return(source_config) }}
{% endmacro %}

{% macro _ods_set_source_type(source_config) %}

    {% set INNER_SOURCES = ['greenplum'] %}
    {% set OUTER_SOURCES = ['postgres', '1c_bi'] %}

    {% if source_config['src_type'] in OUTER_SOURCES %}
        {% do source_config.update({'is_src_external' : true }) %}
    {% elif source_config['src_type'] in INNER_SOURCES %}
        {% do source_config.update({'is_src_external': false }) %}
    {% else %}
        {% do adapter.raise_compiler_error("ERROR: Unknown data source type: " ~ source_config['src_type'] ~ ".") %}
    {% endif %}

    {{ return(source_config) }}
{% endmacro %}

{% macro greenplum__ods_generate_model_config(model_config=config) %}

    {# Separate logic to parse and quote input column names #}
    {% set distributed_by       = greenplum__get_config_colnames('distributed_by', model_config) %}
    {% set load_key             = greenplum__get_config_colnames('load_key', model_config) %}
    {% set cut_field            = greenplum__get_config_colnames('increment_fields', model_config) %}
    {% set partition_column_nm  = greenplum__get_config_colnames('partition_column_nm', model_config) %}
    {% set cdc_type_field       = greenplum__get_config_colnames('cdc_type_field', model_config, 'ce_type') %}
    {% set version_field        = greenplum__get_config_colnames('version_field', model_config) %}
    {% set snapshot_time_field  = greenplum__get_config_colnames('snapshot_time_field', model_config) %}



    {% set model_config = {
        'distributed_by'                 :   distributed_by if distributed_by != '' else None ,
        'cut_field'                      :   cut_field if cut_field != '' else           None ,
        'load_key'                       :   load_key if load_key != ''             else None ,
        'lag_interval'                   :   model_config.get("lag_interval",                  None) ,
        'type_cut_field'                 :   model_config.get("type_cut_field",                None) ,
        'compare_with_full_history'      :   model_config.get("compare_with_full_history",     None) ,
        'partition_count'                :   model_config.get("partition_count",               None) , 
        'partition_column_nm'            :   partition_column_nm if partition_column_nm != '' else None ,
        'min_max_data_from_snp'          :   None                                              ,
        'version_field'                  :   version_field if version_field != '' else None ,
        'snapshot_time_field'            :   snapshot_time_field if snapshot_time_field != '' else None ,
        'version_field_type'             :   model_config.get("version_field_type",    None) ,
        'snapshot_time_field_type'       :   model_config.get("snapshot_time_field_type",    None) ,        
        'cdc_type_field'                 :   cdc_type_field if cdc_type_field != '' else None  ,
        'load_type'                      :   model_config.get('load_type',                     'U'),
        'type_load_strategy'             :   model_config.get('type_load_strategy',           None),
        'addition_tech_fields'           :   model_config.get('addition_tech_fields',           None)
    } %}
    {% do model_config.update({
        'partition_flg' : model_config['partition_count'] != None 
                            and model_config['partition_count'] != "" 
                            and model_config['partition_count'] != "None" 
                            and model_config['partition_count'] > 1 
                            and model_config['partition_column_nm'] != None 
                            and model_config['partition_column_nm'] != "" 
                            and model_config['partition_column_nm'] != "None"
    }) %}
    {{ return(model_config) }}
{% endmacro %}



{% macro _ods_set_tmp_table_fields_custom_default(relations_map, model_config, meta_table) %}

    {% set tech_columns = '' %} 

    {% set business_attrs = greenplum__ods_meta_get_ddl_string(meta_table, pxf_ddl=False, get_dtypes=False) %}
    {% do relations_map.get('tmp').update({
        'business_attrs': business_attrs,
        'tech_columns_src_to_tmp'  : tech_columns
    }) %}
    {{ return(relations_map) }}
{% endmacro %}

{% macro _ods_set_tmp_table_fields_jdbc_snap(relations_map, model_config, meta_table) %}

    {% set tech_columns = ',now()::timestamp(6) as cdc_source_dttm,now()::timestamp(6) as cdc_target_dttm' %} 

    {% set business_attrs = greenplum__ods_convert_pxf_to_gp_fields(meta_table) %}
    {% do relations_map.get('tmp').update({
        'business_attrs': business_attrs,
        'tech_columns_src_to_tmp'  : tech_columns
    }) %}
    {{ return(relations_map) }}
{% endmacro %}


{% macro _ods_set_tmp_table_fields_es(relations_map, model_config, meta_table) %}

    {% set business_attrs = greenplum__ods_meta_get_ddl_string(meta_table, pxf_ddl=False, get_dtypes=False) %}

    {# Exclude cut_fields being part of business_attrs already to avoid double-include. #}
    {%- set tech_columns -%}
            ,{{_ods_get_record_status_convert_query(model_config['cdc_type_field'])}}
            {%- if model_config['cut_field'] -%}
                ,{{ model_config['cut_field'].split(",") | reject('in', business_attrs.split(",")) | join(",") }} 
            {%- endif -%}
            , {{model_config['version_field']}} ::{{model_config['version_field_type']}} as ce_version
            ,ce_time::timestamp(6)
            ,now() as cdc_target_dttm
    {%- endset -%}

    {% do relations_map.get('tmp').update({
        'business_attrs': business_attrs,
        'tech_columns_src_to_tmp'  : tech_columns
    }) %}
    {{ return(relations_map) }}
{% endmacro %}


{% macro _ods_set_tmp_table_fields_msg_lib(relations_map, model_config, meta_table) %}

    {% set business_attrs = greenplum__ods_meta_get_ddl_string(meta_table, pxf_ddl=False, get_dtypes=False) %}

    {# Exclude cut_fields being part of business_attrs already to avoid double-include. #}
    {%- set tech_columns -%}
        {%- if model_config['cut_field'] -%}
            ,{{ model_config['cut_field'].split(",") | reject('in', business_attrs.split(",")) | join(",") }} 
        {%- endif -%}
        ,{{model_config['snapshot_time_field']}}::{{model_config['snapshot_time_field_type']}}
    {%- endset -%}

    {% do relations_map.get('tmp').update({
        'business_attrs': business_attrs,
        'tech_columns_src_to_tmp'  : tech_columns
    }) %}

    {{ return(relations_map) }}
{% endmacro %}

{% macro _ods_set_tmp_table_fields_s3_incr(relations_map, model_config, meta_table) %}

    {% set business_attrs = greenplum__ods_meta_get_ddl_string(meta_table, pxf_ddl=False, get_dtypes=False) %}

    {# Exclude cut_fields being part of business_attrs already to avoid double-include. #}
    {%- set tech_columns -%}
        {%- if model_config['cut_field'] -%}
            ,{{ model_config['cut_field'].split(",") | reject('in', business_attrs.split(",")) | join(",") }} 
        {%- endif -%}
        ,{{model_config['version_field']}}::{{model_config['version_field_type']}}
    {%- endset -%}

    {% do relations_map.get('tmp').update({
        'business_attrs': business_attrs,
        'tech_columns_src_to_tmp'  : tech_columns
    }) %}

    {{ return(relations_map) }}
{% endmacro %}

{% macro _ods_set_snp_tech_fields(relations_map, model_config) %}
    {% set SNP_TECH_FIELDS = [
        api.Column('snp_delete_flg',    'smallint'                          ).get_instance_with_quotated_name()
    ] %}
    {% do relations_map.get('snp').update({'tech_fields': SNP_TECH_FIELDS}) %}
    {{ return(relations_map) }}
{% endmacro %}

{% macro _ods_set_snp_tech_fields_es(relations_map, model_config) %}
    {% set relations_map = _ods_set_snp_tech_fields(relations_map, model_config) %}
    {% do relations_map.snp.tech_fields.extend([
        api.Column('ce_version',        model_config['version_field_type']  ).get_instance_with_quotated_name(),
        api.Column('ce_time',           'timestamp without time zone',       datetime_precision = 6       ).get_instance_with_quotated_name()
    ]) %}
    {{ return(relations_map) }}
{% endmacro %}

{% macro _ods_set_snp_tech_fields_msg_lib(relations_map, model_config) %}
    {% set relations_map = _ods_set_snp_tech_fields(relations_map, model_config) %}
    {% do relations_map.snp.tech_fields.extend([
        api.Column(model_config['snapshot_time_field'], model_config['snapshot_time_field_type']).get_instance_with_quotated_name()
    ]) %}
    {{ return(relations_map) }}
{% endmacro %}

{% macro _ods_set_snp_tech_fields_s3_incr(relations_map, model_config) %}
    {% set relations_map = _ods_set_snp_tech_fields(relations_map, model_config) %}
    {% do relations_map.snp.tech_fields.extend([
        api.Column(model_config['version_field'],    model_config['version_field_type']  ).get_instance_with_quotated_name()
    ]) %}
    {{ return(relations_map) }}
{% endmacro %}

{% macro _ods_set_cdc_tech_fields(relations_map, model_config) %}

    {% set CDC_TECH_FIELDS = [
            api.Column('cdc_type_code',     'character varying', 1          ).get_instance_with_quotated_name(),
            api.Column('cdc_source_dttm',   'timestamp without time zone',   datetime_precision = 6   ).get_instance_with_quotated_name(),
            api.Column('cdc_target_dttm',   'timestamp without time zone',   datetime_precision = 6   ).get_instance_with_quotated_name()
    ]%}
    {% do relations_map.get('cdc').update({'tech_fields': CDC_TECH_FIELDS}) %}
    {{ return(relations_map) }}
{% endmacro %}

{% macro _ods_set_cdc_tech_fields_es(relations_map, model_config) %}
    {% set relations_map = _ods_set_cdc_tech_fields(relations_map, model_config) %}
    {% do relations_map.cdc.tech_fields.extend([
        api.Column('ce_version',    model_config['version_field_type']  ).get_instance_with_quotated_name()
    ]) %}
    {{ return(relations_map) }}
{% endmacro %}

