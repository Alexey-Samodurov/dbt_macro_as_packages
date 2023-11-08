{% macro greenplum__ods_validate_configs(
    relations_map,
    source_config,
    model_config,
    meta_table,
    constants_map
)%}

    {% set validation_scenario = [
        _ods_meta_contains_single_record(meta_table),
        _ods_validate_distribution_by(model_config['distributed_by']),
        _ods_validate_load_key(model_config['load_key']),
        _ods_validate_src_type(source_config['src_type']),
        _ods_validate_increment(model_config['cut_field'], model_config['type_cut_field']),
        _ods_validate_partition_cut_param_exist(relations_map.cdc.partition, model_config['cut_field']),
        _ods_validate_partition_cut_param_exist(relations_map.snp.partition, model_config['cut_field']),
        _ods_validate_partition_have_one_column(relations_map.cdc.partition),
        _ods_validate_partition_have_one_column(relations_map.snp.partition),
        _ods_validate_load_type_strategy(model_config['type_load_strategy'])
    ] %}

    {# Increment and partitioning parameters validation #}
    {% if model_config['cut_field'] != None %}
        {% do validation_scenario.append(_ods_validate_compare_lists_length_equal(model_config['cut_field'].split(","), model_config['type_cut_field'].split(",")))%}

        {% if model_config['lag_interval'] != None %}
            {% do validation_scenario.append(_ods_validate_compare_lists_length_equal(model_config['cut_field'].split(","), model_config['lag_interval'].split(",")))%}
        {% endif %}
    {% endif %}

    {% if not source_config['is_src_external'] %}
        {% set validation_scenario = _ods_validate_add_inner_tests(validation_scenario, relations_map, model_config) %}
    {% endif %}

    {% if source_config['is_src_external'] %}
        {% set validation_scenario = _ods_validate_add_outer_tests(validation_scenario, source_config, meta_table) %}
    {% endif %}

    {% if constants_map.macro_additional_validate != 'skip' %}
        {% set runnable_macro = context.get(constants_map.macro_additional_validate) %}
        {% set validation_scenario =  runnable_macro(validation_scenario, model_config) %}
    {% endif %}

    {% for param_test in validation_scenario %}
        {% do param_test %}
    {% endfor %}
{% endmacro %}

{% macro _ods_validate_add_outer_tests(validation_scenario, source_config, meta_table) %}
    {% do validation_scenario.append(_ods_validate_src_schema_name(source_config['src_schema_name'])) %}
    {% do validation_scenario.append(_ods_validate_src_db_name(source_config['src_db_name'])) %}
    {% do validation_scenario.append(_ods_validate_connection_string(source_config['connection_string'])) %}
    {% do validation_scenario.append(_ods_validate_pxf_user(source_config['pxf_user'])) %}
    {% do return (validation_scenario) %}
{% endmacro %}

{% macro _ods_validate_add_inner_tests(validation_scenario, relations_map, model_config) %}
    {% do validation_scenario.append(_ods_validate_src_relation(relations_map.src.relation)) %}
    {# do validation_scenario.append(_ods_validate_inner_source_cut_fields(model_config['cut_field'])) #}
    {% do return (validation_scenario) %}
{% endmacro %}

{% macro _ods_validate_add_es_tests(validation_scenario, model_config) %}
    {% do validation_scenario.append(_ods_validate_es_cdc_type_field(model_config['cdc_type_field'])) %}
    {% do return (validation_scenario) %}
{% endmacro %}

{% macro _ods_validate_add_msg_lib_tests(validation_scenario, model_config) %}
    {% do validation_scenario.append(_ods_validate_msg_lib_snapshot_time_field(model_config['snapshot_time_field'])) %}
    {% do validation_scenario.append(_ods_validate_msg_lib_snapshot_time_field_type(model_config['snapshot_time_field_type'])) %}    
    {% do return (validation_scenario) %}
{% endmacro %}

{% macro _ods_validate_add_s3_incr_tests(validation_scenario, model_config) %}
    {% do validation_scenario.append(_ods_validate_s3_incr_version_field(model_config['version_field'])) %}
    {% do validation_scenario.append(_ods_validate_s3_incr_version_field_type(model_config['version_field_type'])) %}    
    {% do return (validation_scenario) %}
{% endmacro %}

{# ======== Single tests ======== #}

{% macro _ods_meta_contains_single_record(meta_table) %}
  {% set count_records = meta_table|length %}
  {% if count_records != 1 %}
    {% do adapter.raise_compiler_error('META TABLE ERROR: META Table has wrong number of records after update procedure. Expected = 1 Actual=' ~ count_records) %}
  {% endif %}
{% endmacro %}

{% macro _ods_validate_src_relation(src_relation) %}
    {% if load_relation(src_relation) is none %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n Source relation " ~ src_relation ~ " does not exists in cached schemas.") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_distribution_by(distributed_by) %}
    {% if distributed_by is none or distributed_by == "" or distributed_by == "None" %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n Distribution key is null for " ~ this ~ " relation.") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_load_key(load_key) %}
    {% if load_key is none or load_key == "" or load_key == "None" %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n Load key is null for " ~ this ~ " relation.") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_connection_string(connection_string) %}
    {% if connection_string is none or connection_string == "" or connection_string == "None" %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n Connection URL is null for " ~ this ~ " relation.") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_src_type(src_type) %}
    {% if src_type is none or src_type == "" or src_type == "None" %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n SRC type is null for " ~ this ~ " relation source.") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_src_schema_name(src_schema_name) %}
    {% if src_schema_name is none or src_schema_name == "" or src_type == "None" %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n SRC schema name is null for " ~ this ~ " relation.") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_src_db_name(src_db_name) %}
    {% if src_db_name is none or src_db_name == "" or src_db_name == "None" %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n SRC db name is null for " ~ this ~ " relation.") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_pxf_user(pxf_user) %}
    {% if pxf_user is none or pxf_user == "" or pxf_user == "None" %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n PXF user is null. Please, fill in pxf_user param in dbt_project.yml.") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_increment(cut_field, type_cut_field) %}
    {% if (cut_field is not none and cut_field != "" and cut_field != "None") and (type_cut_field is none or type_cut_field == "" or type_cut_field == "None") %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n Type of cut field is null. Please, define cut type(type_cut_field) in config or dbt_project.yml") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_ddl_fields(relation_fields) %}
    {% if relation_fields == None or relation_fields == 'None' or relation_fields == '' %}
        {% do adapter.raise_compiler_error("META TABLE ERROR: Meta table table_ddl is NONE for " ~ this ~ " relation, pxf = " ~ pxf_fields ~ ".") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_compare_lists_length_equal(list1, list2) %}
    {% if list1|length|int != list2|length|int %}
        {% do adapter.raise_compiler_error("META TABLE ERROR: Lengths of two lists are not the same. Compare the length of all input params containing ','(comma).") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_cut_params_length(model_cut_fields, model_type_cut_fields, meta_cut_str) %}
    {% if (model_cut_fields|length != model_type_cut_fields|length or model_type_cut_fields|length != meta_cut_str|length ) %}
        {% do adapter.raise_compiler_error("ERROR: length type_cut_field, cut_fields and cut_param is not equal") %}
    {% endif  %}
{% endmacro %}


{% macro _ods_validate_load_type_strategy(type_load_strategy) %}
    {% if type_load_strategy not in ['s3_incr', 'es', 'jdbc_snap', 'custom', 'message_lib'] %}
        {% do adapter.raise_compiler_error("Invalid type load strategy provided: "~ type_load_strategy ~ ".\nExpected one of: 's3_incr', 'es', 'jdbc_snap', 'custom', 'message_lib'") %}
    {% endif  %}
{% endmacro %}

{% macro _ods_validate_es_cdc_type_field(cdc_type_field) %}
    {% if cdc_type_field is none or cdc_type_field == "" or cdc_type_field == "None" %}
        {% do adapter.raise_compiler_error("ERROR: CDC Type Field should be non empty for inner sources load") %}
    {% endif  %}
{% endmacro %}


{% macro _ods_validate_msg_lib_snapshot_time_field(snapshot_time_field) %}
    {% if snapshot_time_field is none or snapshot_time_field == "" or snapshot_time_field == "None" %}
        {% do adapter.raise_compiler_error("ERROR: snapshot time Field should be non empty for message_lib load") %}
    {% endif  %}
{% endmacro %}

{% macro _ods_validate_msg_lib_snapshot_time_field_type(snapshot_time_field_type) %}
    {% if snapshot_time_field_type is none or  snapshot_time_field_type.lower() not in ['date','timestamp','timestamp(6) without time zone'] %}
        {% do adapter.raise_compiler_error("ERROR: snapshot time Field type should be date or timestamp or timestamp(6) without time zone") %}
    {% endif  %}
{% endmacro %}

{% macro _ods_validate_s3_incr_version_field(version_field) %}
    {% if version_field is none or version_field == "" or version_field == "None" %}
        {% do adapter.raise_compiler_error("ERROR: version Field should be non empty for message_lib load") %}
    {% endif  %}
{% endmacro %}

{% macro _ods_validate_s3_incr_version_field_type(version_field_type) %}
    {% if version_field_type is none or  version_field_type.lower() not in ['date','timestamp','timestamp(6) without time zone'] %}
        {% do adapter.raise_compiler_error("ERROR: version Field type should be date or timestamp or timestamp(6) without time zone") %}
    {% endif  %}
{% endmacro %}



{% macro _ods_validate_inner_source_cut_fields(cut_field) %}
    {% if cut_field is none or cut_field == "" or cut_field == "None" %}
        {% do adapter.raise_compiler_error("ERROR: CUT FIELD should be non empty for inner source load") %}
    {% endif  %}
{% endmacro %}

{% macro _ods_validate_array_less_or_eq_array(smaller_array, bigger_array) %}
    {% if not(smaller_array|length|int <= bigger_array|length|int) %}
        {% do adapter.raise_compiler_error("ERROR: Array comparison: array1 should be less or equal to array 2.") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_partition_cut_param_exist(partition_config_map, cut_field) %}
    {% if (partition_config_map is not none and partition_config_map != {} and partition_config_map != "None") and (cut_field is none or cut_field == "" or cut_field == "None") %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n Partition parameters are not none. Cut parameters are none. In case of partitioning cut params should not be None.") %}
    {% endif %}
{% endmacro %}

{% macro _ods_validate_partition_have_one_column(partition_config_map) %}
    {% if partition_config_map is not none and partition_config_map != {} and partition_config_map != "None" and partition_config_map.colnames.split(",")|length|int > 1 %}
        {% do adapter.raise_compiler_error("ODS PARAM VALIDATION ERROR:\n Two or more partition columns passed to params. There should be only one partition column.") %}
    {% endif %}
{% endmacro %}

{# ======== Warning checks - for log writing only ======== #}

{% macro _ods_validate_partitioning_params(partition_flg, min_data_from_snp) %}
    {% if partition_flg and min_data_from_snp == None %}
        {{ logger(log_message="For " ~ src_relation ~ " partition was ignored. No min_data_from_snp param.", log_level="INFO") }}
    {% endif %}    
{% endmacro %}
