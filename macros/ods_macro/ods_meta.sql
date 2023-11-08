{% macro greenplum__ods_sync_meta() %}

    {% if execute and flags.WHICH in ('run', 'build') %}

        {# Select models meta has to be updated #}
        {% set ods_models = [] -%}  
        {% for node in graph.nodes.values() %}
            {%- if "ods" in node['tags'] and "off_sync_meta" not in node['tags'] and node['resource_type'] ==  "model"-%}
                {%- do ods_models.append(node) -%}
            {%- endif -%}
        {%- endfor %}

        {% set meta_table_cached = run_query(greenplum__ods_get_meta_table_query(return_active=False)) %}
        {% set src_meta_update_cache = {} %}
        {% for model in ods_models %}

            {# Parse parameters #}
            {% set NAME_SPLITTED = model.name.split("_", 1) %}
            {% set is_active_flg = 'false' if 'disabled' in model.config.tags else 'true' %}
            {% set ods_configs = greenplum__ods_model_configure_meta_sync(
                model_database = model.database, 
                model_schema = model.schema, 
                model_name = model.name,
                config = model.config
            ) %}
            {% set constants_map    = ods_configs['constants_map']  %}
            {% set relations_map    = ods_configs['relations_map']  %}
            {% set source_config    = ods_configs['source_config']  %}
            {% set model_config     = ods_configs['model_config']   %}
            {% set meta_table       = ods_configs['meta_table']   %}

            {# Select target model record from meta #}
            {% set meta_table_model_record = meta_table_cached
                                        |selectattr("table_schema", "equalto", relations_map.base.relation.schema)
                                        |selectattr("table_name", "equalto", relations_map.base.relation.name)
                                        |first %}
            
            {# Update distributed_by and is_active field in meta (compare to model file configs) #}
            {% set meta_table_model_record = _ods_meta_update_model_params(
                                                meta_table_record = meta_table_model_record,
                                                base_relation = relations_map.base.relation,
                                                distributed_by = model_config['distributed_by'],
                                                is_active = is_active_flg) %}

            {# Update table ddl in meta (extra queries are required) #}
            {% do _ods_meta_update_ddl(
                    meta_table_record=meta_table_model_record, 
                    source_config=source_config, 
                    relations_map=relations_map, 
                    constants_map=constants_map, 
                    src_meta_update_cache=src_meta_update_cache ) %}
        
        {% endfor %}
    {%- endif -%}

{% endmacro %}

{% macro _ods_meta_update_ddl(meta_table_record, source_config, relations_map, constants_map, src_meta_update_cache) %}

    {% if meta_table_record.is_active|string|lower == 'true' %}

        {% if not source_config['is_src_external'] %}
            {% do _ods_meta_update_ddl_internal(meta_table_record, relations_map, constants_map) %}

        {% elif source_config['is_src_external'] %}
            {% if source_config['src_db_short'] not in src_meta_update_cache.keys()
                or meta_table_record.table_ddl == none 
                or meta_table_record.table_ddl_pxf == none %}

                {% if source_config['src_db_short'] not in src_meta_update_cache.keys() %}
                    {% set ods_schema_update_query = _ods_meta_update_ddl_external(source_config) %}
                    {% do src_meta_update_cache.update({source_config['src_db_short']: ods_schema_update_query}) %}
                {% endif %}
                
                {% set ext_update_query = src_meta_update_cache.get(source_config['src_db_short'], None) %}
                {% if ext_update_query != None %}
                    {% do run_query(src_meta_update_cache.get(source_config['src_db_short'])) %}
                {% endif %}
            {% endif %}
        {% endif %}

    {% else %}
        {{ logger(log_message="No ddl sync for '" ~ relations_map.base.relation ~ "' due to non-active status.", log_level="INFO") }}
    {% endif %}
{% endmacro %}

{% macro _ods_get_meta_tech_fields(include_fields_list, exclude_fields_list) %}
    {# /* Do not quote these colnames because they will be compared to non-quoted colnames */ #}
    {% set tech_fields = [ 'dataflow_dttm', 'dataflow_id', 'hashdiff_key', 
                            'cdc_source_dttm', 'cdc_transfer_dttm', 'cdc_target_dttm', 'ce_specversion', 
                            'ce_id', 'ce_source', 'ce_type', 'ce_datacontenttype', 'ce_dataschema', 
                            'ce_subject', 'ce_time', 'ce_version', 'ce_data', 'key', 'headers', 
                            'timestamp', '_partition', 'kafka_key', 'kafka_headers', '_gp_partition'
                        ] %}

    {% if include_fields_list != None %}
        {{ tech_fields.extend(include_fields_list) }}
    {% endif %}

    {{ return(tech_fields | reject('in', exclude_fields_list) | unique | list) }}
{% endmacro %}

{% macro _ods_meta_update_ddl_internal(meta_table_record, relations_map, constants_map) %}

    {% set exclude_fields = _ods_get_meta_tech_fields(
                                include_fields_list = constants_map['TECH_FIELDS']|map("replace", '"', '')|list,
                                exclude_fields_list = constants_map['important_tech_fields_list']|map("replace", '"', '')|list) %}
                                                        
    {% set ddl_exists = false if meta_table_record.table_ddl|string|length == 0 else true %}
    {% if not ddl_exists %}
        {% do _ods_meta_set_ddl_from_gp_table(relations_map.base.relation, relations_map.src.relation, exclude_fields) %}
    {% else %}
        {% set count_diffs = greenplum__ods_count_changes_from_meta(
                    relations_map.src.relation, 
                    relations_map.base.relation, 
                    exclude_fields) 
        %}
        {% if count_diffs > 0 %}
            {% do _ods_meta_set_ddl_from_gp_table(relations_map.base.relation, relations_map.src.relation, exclude_fields) %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro _ods_meta_set_ddl_from_gp_table(target_table_in_meta, ddl_source_relation, fields_to_exclude=[]) %}
    {% set update_ddl_query %}
    with extract_meta as (
        select 
            table_schema, table_name,
            string_agg(
                column_name || ' ' || 
                    case
                        when character_maximum_length is not null
                            then data_type || '(' || character_maximum_length || ')'
                        when datetime_precision is not null and data_type not in ('date', 'interval')
                        	then substring(data_type from 1 for position(' ' in data_type)-1 ) || '(' || datetime_precision || ')' || substring(data_type from position(' ' in data_type) )
                            else data_type end,
                ','
                order by ordinal_position::int
            ) as table_ddl
        from information_schema.columns
        where table_schema = '{{ ddl_source_relation.schema }}'
            and table_name = '{{ ddl_source_relation.name }}'
            {% if fields_to_exclude|length >= 1 %} and column_name not in ('{{fields_to_exclude|join("','")}}') {% else %} {% endif %}
        group by table_schema, table_name
    )
    UPDATE grp_meta.ods_meta om
    SET table_ddl = em.table_ddl,
        table_ddl_pxf = null
    FROM extract_meta em
    WHERE om.table_schema = '{{ target_table_in_meta.schema }}'
        and om.table_name = '{{ target_table_in_meta.identifier }}'
    {% endset %}

    {% do run_query(update_ddl_query) %}
{% endmacro %}

{% macro _ods_meta_update_ddl_external(source_config) %}

    {% set TMP_SCHEMA = 'grp_meta' %}
    {% set FOREIGN_PATH = 'query:get_tables_ddl_postgres' %}
    {% set EXT_META_FIELDS = 'schema_name text,table_name text,columns_csv text' %}
    {{ logger(log_message="Sync source '" ~ source_config['src_db_name'] ~ "' in progress.", log_level="INFO") }}

    {% set pxf_connection_string = greenplum__create_pxf_connection_string(
                                    FOREIGN_PATH, 
                                    source_config['src_type'], 
                                    source_config['connection_string'], 
                                    source_config['src_db_name'], 
                                    source_config['pxf_user'] )%}

    {% set ext_meta_relation = api.Relation.create(
                                schema=TMP_SCHEMA, 
                                identifier="tables_ddl_"~source_config['src_db_short']) %}

    {% do greenplum__drop_table(ext_meta_relation, is_external=true, if_exists=true) %}
    {% do run_query(greenplum__create_external_table_as(ext_meta_relation, EXT_META_FIELDS, pxf_connection_string)) %}

    {% if greenplum__ods_is_ext_table_available(ext_meta_relation) != 1 %}
        {{ logger(log_message="External source '"~ source_config['src_db_name'] ~ " is not aviable for meta parsing. External table_ddl is not available.", log_level="WARNING") }}
        {{ return(None) }}
    {% else %}
        {% set update_meta_query %}
            WITH a AS (
                SELECT
                    '{{ source_config['src_db_short'] }}' as source_nm
                    ,schema_name
                    ,table_name
                    ,columns_csv
                    ,regexp_split_to_array((regexp_split_to_table(columns_csv, chr(10))), ',') col
                FROM {{ ext_meta_relation }}
            ),
            b AS (
                SELECT
                    source_nm
                    ,schema_name
                    ,table_name
                    ,col [1] col_num
                    ,col [2] col_name
                    ,col [3] col_type
                    ,CASE
                      WHEN col [3] ~ 'character varying.*\[\]'
                        THEN 'text'
                      WHEN col [3] LIKE 'character varying%'
                        THEN col [3]
                      WHEN col [3] = 'double precision'
                        THEN 'numeric'
                      WHEN (col [3] LIKE 'numeric(%' or col [3] = 'numeric')
                        THEN 'numeric'
                      WHEN col [3] = 'bigserial'
                        THEN 'bigint'
                      WHEN col [3] = 'serial'
                        THEN 'integer'
                      WHEN col [3] LIKE 'timestamp(%)' OR col [3] LIKE 'timestamp(%) without time zone'
                        THEN 'timestamp'
                      WHEN col [3] = 'timestamp with time zone' OR col[3] like 'timestamp(%) with time zone'
                        THEN 'text'
                      WHEN col [3] NOT IN ('integer', 'bigint', 'smallint', 'real', 'float8', 'numeric', 'boolean', 'varchar', 'text', 'date', 'timestamp', 'bytea', 'timestamp without time zone')
                        THEN 'text'
                        ELSE col [3]
                    END col_type_pxf
                FROM a
            ),
            c AS (
                SELECT
                    *
                    ,CASE
                        WHEN col_type IN ('jsonb', 'json')
                            THEN col_type
                        WHEN col_type = 'timestamp' OR col_type = 'timestamp without time zone'
                          	THEN 'timestamp(6) without time zone'
                        WHEN col_type = 'timestamp with time zone'
                            THEN 'timestamp(6) with time zone'
                        WHEN col_type LIKE 'timestamp(%)' OR col_type LIKE 'timestamp(%) without time zone' OR col_type LIKE 'timestamp(%) with time zone'
                          	THEN  'timestamp(' || substring(col_type FROM position('(' IN col_type) + 1 FOR 1) || substring(col_type from position(')' in col_type) )
                        WHEN col_type = 'time' OR col_type = 'time without time zone'
                          	THEN 'time(6) without time zone'
                        WHEN col_type = 'time with time zone'
                            THEN 'time(6) with time zone'
                        WHEN col_type LIKE 'time(%)' OR col_type LIKE 'time(%) without time zone' OR col_type LIKE 'time(%) with time zone'
                          	THEN  'time(' || substring(col_type FROM position('(' IN col_type) + 1 FOR 1) || substring(col_type from position(')' in col_type) )
                        ELSE col_type_pxf
                    END col_type_gp
                FROM b
            ),
            d as (
                SELECT
                    source_nm,
                    schema_name,
                    table_name,
                    string_agg(
                        col_name || ' ' || col_type_gp,
                        ','
                        order by col_num::int
                    ) as table_ddl,
                    string_agg(
                        col_name || ' ' || col_type_pxf,
                        ','
                        order by col_num::int
                    ) as table_ddl_pxf
                FROM c
                GROUP BY source_nm, schema_name, table_name
            ),
            e as (
                SELECT
                    om.table_schema || '.' || om.table_name as table_full_name,
                    om.table_schema,
                    om.table_name,
                    d.table_ddl,
                    d.table_ddl_pxf
                FROM d
                JOIN grp_meta.ods_meta om
                    ON 'grp_ods_'||d.source_nm = om.table_schema
                        AND d.table_name = om.table_name
                WHERE (case
                        when d.table_ddl = coalesce(om.table_ddl, '1') 
                            and d.table_ddl_pxf = coalesce(om.table_ddl_pxf, '1')
                        then true else false end) = false 
            )
            UPDATE grp_meta.ods_meta om
            SET table_ddl = e.table_ddl,
                table_ddl_pxf = e.table_ddl_pxf
            FROM e
            WHERE om.table_schema || '.' || om.table_name =  e.table_full_name
        {% endset %}
        {{ return(update_meta_query) }}
        {# do run_query(update_meta_query) #}
    {% endif %}

{% endmacro %}

{% macro _ods_meta_update_model_params(meta_table_record, base_relation, distributed_by, is_active) %}

    {% set result_meta_table_record = meta_table_record %}
    {% set record = true if meta_table_record|length > 0 else false %}
    {% if record %} 
        {% if (meta_table_record.distribution_key|string != distributed_by)|as_bool
            or (meta_table_record.is_active|string|lower != is_active|lower)|as_bool
        %}
            {% set upd_rec %}
                update grp_meta.ods_meta 
                set distribution_key = '{{ distributed_by }}',
                    is_active = {{ is_active }}::boolean
                where table_schema = '{{ base_relation.schema }}'
                    and table_name = '{{ base_relation.name }}'
            {% endset %}
            {% do run_query(upd_rec) %}

        {% set result_meta_table_record = run_query(greenplum__ods_get_meta_table_query(meta_relation = base_relation, return_active=False))|first %}
        {% endif %}
    {% else %}
        {% set ins_rec %}
            insert into grp_meta.ods_meta (table_schema, table_name, table_ddl, table_ddl_pxf, distribution_key, is_active)
            values('{{ base_relation.schema }}','{{ base_relation.name }}',null,null,'{{ distributed_by }}',{{ is_active }}::boolean )
        {% endset %}
        {% do run_query(ins_rec) %}

        {% set result_meta_table_record = run_query(greenplum__ods_get_meta_table_query(meta_relation = base_relation, return_active=False))|first %}
    {% endif %}

    {{ return(result_meta_table_record) }}
{% endmacro %}

{# ================================ Macro to work with meta data ================================ #}

{% macro greenplum__ods_get_meta_table_query(meta_relation=None, return_active=True) %}
    select 
      table_schema,
      table_name,
      table_ddl,
      table_ddl_pxf,
      distribution_key,
      is_active
    from grp_meta.ods_meta
    where 1=1
      {% if return_active %} and is_active = true {% endif %}
      {% if meta_relation != None %}
        and table_schema = '{{ meta_relation.schema }}'
        and table_name = '{{ meta_relation.name }}'
      {% endif %}
{% endmacro %}

{# Return processed value from table_ddl or table_ddl_pxf columns from ods meta table #}
{# Input    : meta_table - agate.Table, pxf_ddl - bool, get_dtypes - bool #}
{# Return   : quoted table_ddl or table_ddl_pxf columns data either with or without data types   #}
{% macro greenplum__ods_meta_get_ddl_string(meta_table, pxf_ddl=False, get_dtypes=True) %}

  {% if pxf_ddl %}
    {% set relation_fields = meta_table.columns.table_ddl_pxf|first|string %}
  {% else %}
    {% set relation_fields = meta_table.columns.table_ddl|first|string %}
  {% endif %}

  {% do _ods_validate_ddl_fields(relation_fields) %}

  {% set validated_relation_fields = greenplum__ods_meta_ddl_process(relation_fields, ",", '"', get_dtypes) %}
  {{ return(validated_relation_fields) }}
{% endmacro %}

{# Table ddl processor. Quote and drop dtypes if needed #}
{% macro greenplum__ods_meta_ddl_process(ddl_string, delimiter, quotation_symbol="#", get_dtypes=True) %}
  {% set result_array = [] %}

  {% for input_field in ddl_string.split(delimiter) %}
    {% set column_info = input_field.split(" ",1) %}
    {% if get_dtypes %}
        {% do result_array.append(adapter.quote(column_info[0]) ~ " " ~ column_info[1]) %}
    {% else %}
        {% do result_array.append(adapter.quote(column_info[0])) %}
    {% endif %}
  {% endfor %}  

  {{ return(result_array|join(",")) }}
{% endmacro %}

{# Duplicates greenplum__get_columns_in_relation but for ods meta table #}
{# Input    : search_relation - relation (name and schema of table needed to be extracted from meta) #}
{# Return   : list of api.Column objects #}
{% macro greenplum__ods_meta_get_table_ddl(search_relation) %}

    {% set ddl_field = 'table_ddl_pxf' if search_relation.name.endswith("_ext") else 'table_ddl' %}
    {% call statement('get_meta_record', fetch_result=True, auto_begin=False) %}
      select 
        src.column_name, 
        src.data_type
      from (
        select
          substring(colname_dtype, '^\S+') as column_name,
          substring(colname_dtype, '^\S+\s+(.*)') as data_type,
          ordinal_position
        from (  
          select
            unnest(string_to_array(table_ddl, ',')) as colname_dtype,
            generate_subscripts(string_to_array( {{ddl_field}} , ','), 1) AS ordinal_position
          from grp_meta.ods_meta
          where table_schema = '{{ search_relation.schema }}'
              and table_name = '{{ search_relation.name }}'
        ) unnst
      ) as src
    {% endcall %}
    {% set table = load_result('get_meta_record').table %}
    {{ return( greenplum__sql_convert_columns_in_relation(table) ) }}
{% endmacro %}

{% macro greenplum__ods_is_ext_table_available(relation) %}
    {% call statement('get_avlb_ext_table', fetch_result=True, auto_begin=False) %}
        select grp_meta.dbt_check_avbl_ext_table('{{ relation.schema_nq }}.{{ relation.identifier_nq }}') avl;
    {% endcall %}

    {% set res = load_result('get_avlb_ext_table').table.columns.avl|first|int %}
    {% do return(res) %}
{% endmacro %}