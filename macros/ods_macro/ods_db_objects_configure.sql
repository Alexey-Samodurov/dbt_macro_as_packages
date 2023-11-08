{# ================================ DB Objects configuration ================================ #}

{% macro greenplum__ods_model_configure_db_elements(meta_table, relations_map, source_config, model_config, constants_map) %}

    {# Schema check #}
    {% do greenplum__create_schema(relations_map.base.relation) %}

    {# If src is external then check/create/replace external table #}
    {% if source_config['is_src_external'] %}
        {% do _ods_create_or_check_ext_table(meta_table, relations_map, source_config) %}
    {% endif %}

    {# create cdc and snp tables #}
    {% set ods_payload_fields = greenplum__ods_meta_get_ddl_string(meta_table, pxf_ddl=False, get_dtypes=True) %}
    {% do _ods_create_snp_table(relations_map, ods_payload_fields, model_config) %}
    {% do _ods_create_cdc_table(relations_map, ods_payload_fields, model_config) %} 

    {# Calculate params based on created objects #}
    {# Where_str = increment identitfication #}
    {# increment_hash = hash of temp table #}
    {# target_hash = hash of snp table #}
    {% do _ods_set_where_string(
                model_config, 
                source_config['is_src_external'], 
                relations_map.cdc.relation, 
                relations_map.src.relation) %}
    {% do _ods_set_target_partition_prunning_str(
                model_config, 
                relations_map, 
                source_config) %}
    {% do _ods_set_increment_hash_attributes(
                relations_map,
                constants_map['TECH_FIELDS']) %}
    {% do _ods_set_target_hash_attributes(
                relations_map,
                constants_map['TECH_FIELDS'] ) %}

{% endmacro %}

{# ================================ DB Object creators ================================ #}
{% macro _ods_create_or_check_ext_table(meta_table, relations_map, source_config) %}

    {# Create pxf connection string  #}
    {% set pxf_connection_string = greenplum__create_pxf_connection_string(
        source_config['src_schema_name'] ~ "." ~ relations_map.base.relation.name, 
        source_config['src_type'], 
        source_config['connection_string'], 
        source_config['src_db_name'], 
        source_config['pxf_user'],
        True
    ) %}

    {% set ext_table_exists = load_relation(relations_map.src.relation) is not none %}
    {% set ext_relation_fields = greenplum__ods_meta_get_ddl_string(meta_table, pxf_ddl=True, get_dtypes=True) %}

    {# ODS ext table creation #}
    {% if not ext_table_exists %}
        {% do run_query(greenplum__create_external_table_as(
                            relations_map.src.relation, 
                            ext_relation_fields, 
                            pxf_connection_string)) %}
    {% else %}
        {% set changed_cols_cnt = greenplum__ods_count_changes_from_meta(
                                    relations_map.src.relation, 
                                    relations_map.base.relation) %}
        {% if changed_cols_cnt != 0 %}
            {% do greenplum__drop_table(relations_map.src.relation, is_external=true, if_exists=true) %}
            {% do run_query(greenplum__create_external_table_as(
                                relations_map.src.relation, 
                                ext_relation_fields, 
                                pxf_connection_string)) %}
        {% endif %}  
    {% endif %}
{% endmacro %}



{% macro _ods_create_snp_table(relations_map, relation_fields, model_config) %}
    {% set SNP_TECH_FIELDS = [] %}
    {%- do SNP_TECH_FIELDS.extend(relations_map.snp.tech_fields) -%}

    {% set snp_tech_fields_string %}
        {%- for field in SNP_TECH_FIELDS -%} 
            {{ ',' if loop.first else '' }}{{field.name}} {{field.data_type}}{{ '' if loop.last else ',' }}
        {%- endfor -%}
    {% endset %}

    {# /* Create table if not exists. */ #}
    {% set snp_table_exists = load_relation(relations_map.snp.relation) is not none %}
    {% if not snp_table_exists %}
        {% do run_query(greenplum__create_empty_table(
            False, 
            relations_map.snp.relation, 
            fields=relation_fields ~ snp_tech_fields_string,
            add_sys_fields = true,
            custom_distributed_by = model_config['distributed_by'],
            partition_column = relations_map.snp.partition.get('colnames', none)
        )) %}
    {% endif %}

    {% do run_query(greenplum__grant_select_pd(relations_map.snp.relation, [])) %}

    {% if model_config['partition_flg'] %}
        {% if snp_table_exists %}
            {% do model_config.update({'min_max_data_from_snp' : greenplum__get_min_max_part_column_query(model_config['partition_column_nm'], relations_map.snp.relation)}) %}
        {% endif %}         
        {% if model_config['min_max_data_from_snp'] == None  %} 
            {% do model_config.update({'min_max_data_from_snp' : greenplum__get_min_max_part_column_query(model_config['partition_column_nm'], relations_map.src.relation)}) %}
        {% endif %}
    {% endif %}

    {# /* Compare expected columns (meta-business-columns+snp-specific-tech-columns) to real columns (that exist in snp table) */ #}
    {% set snp_relation_columns = greenplum__get_columns_in_relation(relations_map.snp.relation) %}
    {% set meta_columns = [] %}
    {% do meta_columns.extend(relations_map.base.columns) %}
    {% do meta_columns.extend(SNP_TECH_FIELDS) %}

    {% set cnt_missing_cols = greenplum__ods_add_missing_columns(   
                                relations_map.snp.relation,
                                meta_columns, 
                                snp_relation_columns ) %}

    {% set cnt_corrected_dtypes = greenplum__ods_fix_columns_dtypes (
                                        relations_map.snp.relation,
                                        meta_columns, 
                                        snp_relation_columns ) %}

    {# /* Update info about columns if any changes were made. */ #}
    {% if cnt_missing_cols > 0 or cnt_corrected_dtypes > 0 %}
        {% do relations_map.get('snp').update({
                'columns' : greenplum__get_columns_in_relation(relations_map.snp.relation)
        }) %} 
    {% else %}
        {% do relations_map.get('snp').update({
                'columns' : snp_relation_columns
        }) %} 
    {% endif %}
{% endmacro %}

{% macro _ods_create_cdc_table(relations_map, relation_fields, model_config) %}

    {# /* Parse extra cdc-specific tech columns. */ #}
    {% set CDC_TECH_FIELDS = relations_map.cdc.tech_fields %}
    {% set cdc_tech_fields_string %}
        {%- for field in CDC_TECH_FIELDS -%} 
            {{ ',' if loop.first else '' }}{{field.name}} {{field.data_type}}{{ '' if loop.last else ',' }}
        {%- endfor -%}
    {% endset %}
    
    {% set cdc_table_exists = load_relation(relations_map.cdc.relation) is not none %}
    {% if not cdc_table_exists %}
        {% do run_query(greenplum__create_empty_table(
            False, 
            relations_map.cdc.relation, 
            fields=relation_fields ~ cdc_tech_fields_string,
            add_sys_fields = true,
            custom_distributed_by = model_config['distributed_by'],
            partition_column = relations_map.cdc.partition.get('colnames', none)
        )) %}
    {% endif %}

    {% do run_query(greenplum__grant_select_pd(relations_map.cdc.relation,[])) %}

    {# /* Compare expected columns (meta-business-columns+cdc-specific-tech-columns) to real columns (that exist in cdc table) */ #}
    {% set cdc_relation_columns = greenplum__get_columns_in_relation(relations_map.cdc.relation) %}
    {% set meta_columns = [] %}
    {% do meta_columns.extend(relations_map.base.columns) %}
    {% do meta_columns.extend(CDC_TECH_FIELDS) %}

    {% set cnt_missing_cols = greenplum__ods_add_missing_columns(   
                                relations_map.cdc.relation,
                                meta_columns, 
                                cdc_relation_columns ) %}

    {% set cnt_corrected_dtypes = greenplum__ods_fix_columns_dtypes (
                                    relations_map.cdc.relation,
                                    meta_columns, 
                                    cdc_relation_columns ) %}

    {# /* Update info about columns if any changes were made. */ #}
    {% if cnt_missing_cols > 0 or cnt_corrected_dtypes > 0 %}
        {% do relations_map.get('cdc').update({
            'columns' : greenplum__get_columns_in_relation(relations_map.cdc.relation)
        }) %}
    {% else %}
        {% do relations_map.get('cdc').update({
            'columns' : cdc_relation_columns
        }) %}
    {% endif %}
    
{% endmacro %}

{% macro _ods_set_where_string(model_config, is_src_external, cdc_relation, src_relation) %}
    {% do model_config.update(greenplum__generate_where_string(
                                model_config['cut_field'], 
                                model_config['lag_interval'], 
                                cdc_relation if is_src_external else src_relation, 
                                model_config['type_cut_field'])) %}                      
    {{ return(model_config) }}
{% endmacro %}

{% macro _ods_set_target_partition_prunning_str(model_config, relations_map, source_config) %}

    {% if relations_map.snp.partition != {} 
        and relations_map.snp.partition|length|int > 0 
        and model_config.load_type == "A" 
    %}

        {# /* At this point we are pretty sure our partition_colname and cut_field both contain 1 element (see validation logic) */ #}
        {# /* Perform .split(",") in order to get lists of 1 element */ #}
        {% set partition_cols_list = relations_map.snp.partition.colnames.split(",") %}
        {% set cut_params_values = get_cut_param(
                                relations_map.cdc.relation if source_config.is_src_external else relations_map.src.relation
                                ).split(",") %}
        {% set type_cut_fields = model_config.get("type_cut_field").split(",") %}
        
        {% set snp_prunning_conditions %}
            {%- for i in range(0, partition_cols_list|length) -%}
                {%- if type_cut_fields[i] in ("TIMESTAMP", "DATE", "DATE_TO_TEXT") -%}
                    {{ partition_cols_list[i] }}::timestamp >=    '{{ cut_params_values[i] }}'::timestamp without time zone
                {%- elif type_cut_fields[i] in ("BIGINT", "INT","SERIAL","BIGSERIAL","INTEGER") -%}
                    {{ partition_cols_list[i] }}            >=    '{{ cut_params_values[i] }}'::BIGINT
                {%- else -%}
                    {% do adapter.raise_compiler_error("ERROR: Unknown cut type: " ~ type_cut_fields[i] ~ ".") %}
                {%- endif -%}
            {%- endfor -%}
        {% endset %}

        {% do model_config.update({
            'target_partition_prunning_str' : snp_prunning_conditions })
        %}
    {% else %}
        {% do model_config.update({
            'target_partition_prunning_str' : None })
        %}
    {% endif %}
{% endmacro %}

{% macro _ods_get_record_status_convert_query(cdc_type_field) %}
    {%- set record_status_convert_query -%}
        case 
            when  lower({{ cdc_type_field }}) = 'create'    then 'I'
            when  lower({{ cdc_type_field }}) = 'created'   then 'I'
            when  lower({{ cdc_type_field }}) = 'adjust.created'   then 'I'
            when  lower({{ cdc_type_field }}) = 'insert'    then 'I'
            when  lower({{ cdc_type_field }}) = 'inserted'  then 'I'
            when  lower({{ cdc_type_field }}) = 'update'    then 'U'
            when  lower({{ cdc_type_field }}) = 'updated'   then 'U'
            when  lower({{ cdc_type_field }}) = 'delete'    then 'D'
            when  lower({{ cdc_type_field }}) = 'deleted'   then 'D'
            else {{ cdc_type_field }}
        end as {{cdc_type_field}}
    {%- endset -%}
    {{return(record_status_convert_query)}}
{% endmacro %}

{% macro _ods_set_increment_hash_attributes(relations_map, exclude_fields=[]) %}

    {# snp_relation_columns should contain bigger or same number of columns compared to meta #}
    {% do _ods_validate_array_less_or_eq_array(relations_map.base.columns, relations_map.snp.columns) %}

    {% set increment_hash_fields = [] %}
    {% set columns_lj_by_name = greenplum__get_columns_same_name_left_join(relations_map.snp.columns, relations_map.base.columns, exclude_fields) %}
    
    {% for col in columns_lj_by_name %}
        {% if col.name == 'null' or col.data_type == 'text' %}
            {% do increment_hash_fields.append(col.name) %}
        {% else %}
            {% do increment_hash_fields.append(col.name ~ "::" ~ col.data_type) %}
        {% endif %}
    {% endfor %}

    {% do relations_map.get('tmp').update({
        'hash_attributes' : increment_hash_fields
    }) %}
{% endmacro %}

{% macro _ods_set_target_hash_attributes(relations_map, exclude_fields=[]) %}

    {% set target_hash_fields = [] %}
    {% for col in relations_map.snp.columns %}
        {% if col.name not in exclude_fields%}
            {% if col.data_type == 'text' %}
                {% do target_hash_fields.append(col.name) %}
            {% else %}
                {% do target_hash_fields.append(col.name ~ "::" ~ col.data_type) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% do relations_map.get('snp').update({
        'hash_attributes' : target_hash_fields
    }) %}
{% endmacro %}

{# ================================ Helpers Macros ================================ #}

{% macro greenplum__ods_make_relation(base_relation, db='', schema='', prefix='', suffix='') %}
  {% set new_relation_database = base_relation.database if db == '' else db %}
  {% set new_relation_schema = base_relation.schema if schema == '' else schema %}
  {% set new_relation_prefix = prefix ~ "_" if not prefix.endswith("_") and prefix != '' else prefix %}
  {% set new_relation_suffix = "_" ~ suffix if not suffix.startswith("_") and suffix != '' else suffix %}

  {% set prefix_length = new_relation_prefix|length %}
  {% set suffix_length = new_relation_suffix|length %}
  {% set relation_max_name_length = 59 %}

  {% set new_relation_identifier = new_relation_prefix ~ base_relation.identifier[:relation_max_name_length - suffix_length - prefix_length] ~ new_relation_suffix %}
  {% do return(base_relation.incorporate(
    path={
      "database": new_relation_database,
      "schema": new_relation_schema,
      "identifier": new_relation_identifier
    })) 
  %}
{% endmacro %}

{% macro greenplum__ods_choose_env_connection_url(config, env_name) %}
  {% if env_name == 'dev' or env_name == 'test' %}
    {{ return(config.get('src_connection_url_dev', None)) }}
  {% elif env_name == 'prod' or env_name == 'prd' %}
    {{ return(config.get('src_connection_url_prod', None)) }}
  {% else %}
    {% do adapter.raise_compiler_error("ERROR: Unknown environment: " ~ env_name ~ ".") %}
  {% endif %}
{% endmacro %}

{% macro greenplum__ods_convert_pxf_to_gp_fields(meta_table) %}
    {% set result_array = [] %}

    {% set table_ddl = greenplum__ods_meta_get_ddl_string(meta_table, pxf_ddl=false, get_dtypes=True) %}
    {% set table_ddl_list = table_ddl.split(",") %}

    {% set table_ddl_pxf = greenplum__ods_meta_get_ddl_string(meta_table, pxf_ddl=true, get_dtypes=True) %}
    {% set table_ddl_pxf_list = table_ddl_pxf.split(",") %}
    
    {% do _ods_validate_compare_lists_length_equal(table_ddl_list, table_ddl_pxf_list) %}

    {% for colnum in range(table_ddl_list|length) %}
      {% set colname_dtype = table_ddl_list[colnum].split(" ", 1) %} {# [0] - colname, [1] - dtype #}
      {% set colname_dtype_pxf = table_ddl_pxf_list[colnum].split(" ", 1) %}

      {% if colname_dtype[1] != colname_dtype_pxf[1] %}
        {% do result_array.append(colname_dtype[0] ~ "::" ~ colname_dtype[1]) %}
      {% else %}
        {% do result_array.append(colname_dtype[0]) %}
      {% endif %}
    {% endfor %}
    {{ return(result_array|join(",")) }}
{% endmacro %}

{% macro greenplum__generate_where_string(
    cut_field,
    lag_interval,
    cut_record_relation,
    type_cut_field
)
%}
  {% set where_str = "" %}
  {% set where_str_for_src = "" %}
  {% if cut_field != None and cut_field != "" %}

    {% set cut_str = get_cut_param(cut_record_relation).split(',') %}
    {% set type_cut_fields = type_cut_field.split(',') %}
    {% if lag_interval != None %} {%- set lag_interval = lag_interval.split(',') -%} {% endif %}
    {% set cut_fields = cut_field.split(',') %}
    {{ _ods_validate_cut_params_length(model_cut_fields = cut_fields, model_type_cut_fields = type_cut_fields, meta_cut_str = cut_str) }}
	{%- set where_str_inside -%}
      {%- for cut_column in range(0,cut_fields|length) -%}

        {%- set lag_interv = "" -%}
        {%- if lag_interval != None and ( lag_interval[cut_column] != None and lag_interval[cut_column].replace(' ', '') != "" ) -%}
            {%- set lag_interv = _ods_generate_lag_interval(type_cut_fields[cut_column], lag_interval[cut_column]) -%}
        {%- endif -%}
        {{ _ods_generate_where_filter(type_cut_fields[cut_column], lag_interv, cut_fields[cut_column], cut_str[cut_column], False) }}
        {% if not loop.last %} OR {% endif %}
      {%- endfor -%}
    {%- endset -%}

	{%- set where_str_for_src_inside -%}
      {%- for cut_column in range(0,cut_fields|length) -%}

        {%- set lag_interv = "" -%}
        {%- if lag_interval != None and ( lag_interval[cut_column] != None and lag_interval[cut_column].replace(' ', '') != "" ) -%}
            {%- set lag_interv = _ods_generate_lag_interval(type_cut_fields[cut_column], lag_interval[cut_column]) -%}
        {%- endif -%}
        {{ _ods_generate_where_filter(type_cut_fields[cut_column], lag_interv, cut_fields[cut_column], cut_str[cut_column], True) }}
        {% if not loop.last %} OR {% endif %}
      {%- endfor -%}
    {%- endset -%}    
    
  {%- set where_str = where_str_inside -%}
  {%- set where_str_for_src = where_str_for_src_inside -%}
  {% endif %}
  {% do return({'where_str'         : where_str,
                'where_str_for_src' : where_str_for_src}) %}
            
{% endmacro %}

{% macro _ods_generate_where_filter(type_cut_field, lag_interval_value, cut_field, cut_str, pxf_pushdown_flg) %}
        {%- if type_cut_field in ("TIMESTAMP") -%}
            {{ cut_field }}::timestamp > ('{{cut_str}}'::timestamp {{lag_interval_value }})::timestamp

        {%- elif type_cut_field in ("DATE") -%}
            {{ cut_field }}::date > ('{{cut_str}}'::date {{lag_interval_value }})::date
        {# PXF filter pushdown does not work with datatype TIMESTAMPTZ. But it works if field was casted to text #}
        {%- elif type_cut_field in ("TIMESTAMP WITH TIME ZONE") -%}
            {{ cut_field }}> ('{{cut_str}}'::timestamp {{lag_interval_value }}){%- if pxf_pushdown_flg -%}::text {%- endif -%}

        {%- elif type_cut_field in ("BIGINT", "INT","SERIAL","BIGSERIAL","INTEGER") -%}
            {{ cut_field }}> ('{{cut_str}}'::bigint {{lag_interval_value }})

        {# Kind of forbidden magic. Should not be used unless DDL-tool data integration #}
        {%- elif type_cut_field in ("DATE_TO_TEXT") -%}
            {{ cut_field }}> ('{{cut_str}}'::date {{lag_interval_value }})::date::text

        {%- endif -%}
{% endmacro %}


{% macro _ods_generate_lag_interval(type_cut_field, lag_interval_value) %}
        {%- if type_cut_field in ("TIMESTAMP", "DATE", "DATE_TO_TEXT","TIMESTAMP WITH TIME ZONE") -%}
            {{ return("- interval '" ~ lag_interval_value ~ "' day") }}
            
        {%- elif type_cut_field in ("BIGINT", "INT","SERIAL","BIGSERIAL","INTEGER") -%}
            {{ return(" - " ~ lag_interval_value ~ '::bigint') }}

        {%- else -%}
            {% do adapter.raise_compiler_error('LAG GENERATION ERROR: Unknown type_cut_field value: ' ~ type_cut_field ~ ".") %}
        {%- endif -%}
{% endmacro %}