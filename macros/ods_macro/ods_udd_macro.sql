{% macro greenplum__ods_fill_udd_dictionary(relations_map, source_config) %}

    {% set udd_settings_map = {
        'add_un_dictionary_element_map' :   config.get("add_udd_element_map",       False),
        'add_un_dictionary_element'     :   config.get("add_udd_element",           False),
        'include_obs_records'           :   config.get("include_obs_records",       True) ,
        'dictionary_code'               :   config.get("dictionary_code",           None) |upper |trim |string ,
        'element_code_field'            :   greenplum__get_config_colnames("element_code_field", config, replace_value=None),
        'source_element_code_field'     :   greenplum__get_config_colnames("source_element_code_field", config, replace_value=None),
        'eff_from'                      :   greenplum__get_config_colnames("eff_from", config, replace_value="cdc_source_dttm"),
        'ref_fields'                    :   greenplum__get_config_colnames("ref_fields", config, replace_value=None)
    } %}

    {% do greenplum__udd_validate_configs(
        udd_settings_map['add_un_dictionary_element_map'],
        udd_settings_map['add_un_dictionary_element'],
        udd_settings_map['dictionary_code'],
        udd_settings_map['element_code_field'],
        udd_settings_map['source_element_code_field'],
        udd_settings_map['ref_fields'],
        udd_settings_map['include_obs_records'],
        udd_settings_map['eff_from']
    ) %}

    {#/* if flag is True, insert data into <un_dictionary_element_map> */#}
    {% if udd_settings_map['add_un_dictionary_element_map'] %}
        {% do greenplum__udd_insert_registered_values(
            src_relation              = relations_map.cdc.relation,
            dictionary_code           = udd_settings_map['dictionary_code'],
            element_code_field        = udd_settings_map['element_code_field'],
            source_element_code_field = udd_settings_map['source_element_code_field'],
            source_element_desc_cols  = udd_settings_map['ref_fields'],
            source_system_dk          = source_config['src_db_short']|upper|string,
            eff_from                  = udd_settings_map['eff_from'],
            add_to_dictionary_element = udd_settings_map['add_un_dictionary_element'],
            include_obs_records       = udd_settings_map['include_obs_records']
        ) %}
    {% endif %}

    {#/* if flag is True, insert data into <un_dictionary_element> */#}
    {% if udd_settings_map['add_un_dictionary_element'] %}
        {% do greenplum__udd_dictionary_element_update(udd_settings_map['dictionary_code']) %}
    {% endif %}
{% endmacro %}
    
{% macro greenplum__udd_validate_configs(
    add_un_dictionary_element_map,
    add_un_dictionary_element,
    dictionary_code,
    element_code_field,
    source_element_code_field,
    ref_fields,
    include_obs_records,
    eff_from,
    FROM_MAP_TO_DICT_ELEMENT_FLG_NAME
) %}

    {% if add_un_dictionary_element_map is not boolean %}
      {% do adapter.raise_compiler_error("UDD ERROR: config parameter <add_un_dictionary_element_map> should be boolean.") %}
    {% endif %}

    {% if add_un_dictionary_element is not boolean %}
      {% do adapter.raise_compiler_error("UDD ERROR: config parameter <add_un_dictionary_element> should be boolean.") %}
    {% endif %}

    {% if add_un_dictionary_element_map == False and add_un_dictionary_element == True %}
      {% do adapter.raise_compiler_error("UDD ERROR: Invalid parameters: <add_un_dictionary_element_map> is False and <> is True. \nCan not update udd_element_dictionary without updating udd_element_dictionary_map.") %}
    {% endif %}

    {% if add_un_dictionary_element_map == True and
        (   (dictionary_code==None or dictionary_code=='' or dictionary_code=='None')
        or  (element_code_field==None or element_code_field=='' or element_code_field=='None')
        or  (source_element_code_field==None or source_element_code_field=='' or source_element_code_field=='None')
        or  (ref_fields==None or ref_fields=='' or ref_fields=='None')
        )
    %}
        {% do adapter.raise_compiler_error("UDD ERROR: Invalid Parameters: Obligatory UDD configs are empty while <add_udd_dictionary_element_map> = True. Make sure all these parameters have values: <dictionary_code>,<element_code_field>,<source_element_code_field>,<ref_fields>.") %}
    {% endif %}

    {% if add_un_dictionary_element == True and ( dictionary_code==None or dictionary_code=='' or dictionary_code=='None') %}
        {% do adapter.raise_compiler_error("UDD ERROR: Obligatory UDD configs are empty while <add_un_dictionary_element> = True. Make sure all these parameters have values: <dictionary_code>.") %}
    {% endif %}

    {% if include_obs_records is not boolean %}
      {% do adapter.raise_compiler_error("UDD ERROR: <include_obs_records> should be boolean.") %}
    {% endif %}

    {% if eff_from is not string or eff_from == '' %}
        {% do adapter.raise_compiler_error('UDD ERROR: <eff_from> should be string with eff_from column name.') %}
    {% endif %}
{% endmacro %}

{% macro _udd_scd2_map_loader(
  map_relation,
  tmp_snp_relation,
  source_system_dk,
  add_to_dictionary_element,
  dictionary_code,
  include_obs_records
) %}

  {# Input parameters #}
  {%- set ADD_TO_DICTIONARY_ELEMENT_FLG_NAME = 'priority_ref_source' -%}
  {%- set TMP_TABS_SETTINGS_TEXT = 'with (appendonly=true, compresstype=zstd, compresslevel=1, orientation=column)' -%}

  {%- set delete_flag_list        = greenplum__quote_string_array(ADD_TO_DICTIONARY_ELEMENT_FLG_NAME.split(",")|list) -%}
  {%- set join_columns            = greenplum__quote_string_array(['dictionary_code','source_element_code','source_system_dk']) -%}
  {%- set tgt_fields_to_exclude   = greenplum__quote_string_array(['valid_from_dttm', 'dataflow_id']) -%}

  {%- set source_columns    = get_columns_in_relation(tmp_snp_relation)|map(attribute='name')|reject('in', delete_flag_list)|list -%}
  {%- set target_columns    = get_columns_in_relation(map_relation)|map(attribute='name')|reject('in', delete_flag_list + tgt_fields_to_exclude)|list -%}
  {%- set attribute_columns = target_columns|reject('in', join_columns)|list -%}

  {# /* Query to get last record from map by key */ #}
  {% set target_table_query %}
    select distinct on ( {{join_columns|join(", ")|string}} ) 
      {{ target_columns |join(", ") |string }},
      valid_from_dttm as valid_from_dttm_tgt
    from {{ map_relation }}
    where source_system_dk = '{{ source_system_dk }}'
      and dictionary_code = '{{ dictionary_code }}'
    order by {{join_columns|join(", ")|string}}, valid_from_dttm desc
  {% endset %}

  {% set dml %}
    {{ _sat_drop_temp_tables(tmp_snp_relation) }}

    {# Create first temp table #}
    create table {{ _sat_get_temp_table_name(tmp_snp_relation, 1) }} 
      {{ TMP_TABS_SETTINGS_TEXT }} as
    select {{ source_columns|join(', ') }}
    from {{ tmp_snp_relation }} limit 0
    distributed by ( {{ join_columns|join(', ') }} );

    {#/* Load first temp table including ALL new records or records with >= valid_from_dttm */#}
    insert into {{ _sat_get_temp_table_name(tmp_snp_relation, 1) }}
      select 
        t1.{{ source_columns|join(', t1.') }}
      from {{ tmp_snp_relation }} t1 
      left join ({{ target_table_query }}) t2
        on {{ _sat_get_join_conditions(join_columns) }}
      where t1.source_system_dk = '{{ source_system_dk }}'
          and t1.dictionary_code = '{{ dictionary_code }}'
          and (t2.valid_from_dttm_tgt is null or t1.valid_from_dttm >= t2.valid_from_dttm_tgt);
      analyze {{ _sat_get_temp_table_name(tmp_snp_relation, 1) }};

    {#/* Rollup to first records by key and valid_from */#}
    {#/* Compare to target and insert */#}
    with new_records_rollup as (
      select distinct on ({{ target_columns |join(", ") |string }})
        {{ source_columns |join(', ') |string }}
      from {{ _sat_get_temp_table_name(tmp_snp_relation, 1) }} t1
      order by {{ source_columns |join(', ') |string }} asc 
    )
    insert into {{ map_relation }} (
      {{ source_columns|join(', ') }},
      {{ADD_TO_DICTIONARY_ELEMENT_FLG_NAME}},
      dataflow_id 
    )
    select 
      t1.{{ source_columns|join(', t1.') }},
      {%- if add_to_dictionary_element -%}
        true as {{ADD_TO_DICTIONARY_ELEMENT_FLG_NAME}},
      {%- else -%}
        false as {{ADD_TO_DICTIONARY_ELEMENT_FLG_NAME}},
      {%- endif -%}
      '{{ get_dataflow_id() }}' as dataflow_id
    from new_records_rollup t1
    left join ({{ target_table_query }}) t2 
      on {{ _sat_get_join_conditions(join_columns) }}
    where (t2.valid_from_dttm_tgt is null or t1.valid_from_dttm != t2.valid_from_dttm_tgt)
      {% if not include_obs_records %}
      and t1.status_code != 'OBS'
      {% endif %}
    ;

    {{ _sat_drop_temp_tables(tmp_snp_relation) }}
    analyze {{ map_relation }};   

  {% endset %}

  {% do return(dml) %}
{% endmacro %}

{% macro _udd_concat_fields(desc_cols, delimiter='|') %}

  {% if desc_cols == '' %}
    {% set desc_cols_concat = "''" %}
  {% else %}
    {% set desc_cols_concat %}
      coalesce( 
        {% for col in desc_cols.split(",") %} 
          {{ col }}::text, '') 
          {%- if not loop.last -%} || '{{ delimiter }}' || coalesce( {%- endif -%} 
        {% endfor %}    
    {% endset %}
  {% endif %}

  {% do return(desc_cols_concat) %}
{% endmacro %}

{#/* Insert values from source to map */#}
{% macro greenplum__udd_insert_registered_values(
  src_relation,
  dictionary_code,
  element_code_field,
  source_element_code_field,
  source_element_desc_cols,
  source_system_dk,
  eff_from,
  add_to_dictionary_element,
  include_obs_records
) %}
            
  {% set UN_DICTIONARY_ELEMENT_MAP_DIST_BY = 'dictionary_code,source_element_code,source_system_dk' %}
  {% set UN_DICTIONARY_ELEMENT_MAP_RELATION = api.Relation.create(
                                                schema='grp_meta', 
                                                identifier='un_dictionary_element_map') %}
  {% set udd_tmp_relation = make_temp_relation(src_relation) %}

  {#/* Choose data to be inserted into un_dictionary_map */#}
  {#/* Add map with NEW records in order to avoid deletion */#}
  {% set udd_temp_sql %}
    select distinct
      '{{ dictionary_code }}'::varchar(32) as dictionary_code,
      {{ element_code_field }}::varchar(32) as element_code,
      {{ source_element_code_field }}::text as source_element_code,
      {{ _udd_concat_fields(source_element_desc_cols) }}::text as source_element_desc,
      '{{ source_system_dk }}'::text as source_system_dk,
      case 
        when cdc_type_code in ('I', 'U')
                then 'USE'
            when cdc_type_code = 'D'
                then 'OBS'
      end as status_code,
      {{ eff_from }} as valid_from_dttm
    from {{ src_relation }}
    where {{ element_code_field }} is not null  
      and {{ source_element_code_field }} is not null
    UNION ALL 
    select distinct
      dictionary_code,
      element_code,
      source_element_code,
      source_element_desc,
      source_system_dk,
      status_code,
      valid_from_dttm
    from {{ UN_DICTIONARY_ELEMENT_MAP_RELATION }}
    where status_code = 'NEW'
      and source_system_dk = '{{source_system_dk}}'
      and dictionary_code = '{{ dictionary_code }}'
  {% endset %}

  {% do run_query(greenplum__create_table_as(True, udd_tmp_relation, udd_temp_sql, custom_distributed_by=UN_DICTIONARY_ELEMENT_MAP_DIST_BY)) %}
  {% do run_query(_udd_scd2_map_loader(UN_DICTIONARY_ELEMENT_MAP_RELATION, udd_tmp_relation, source_system_dk, add_to_dictionary_element, dictionary_code, include_obs_records)) %}

{% endmacro %}


{# /* The following macros check incoming table for NEW records and save to map. */ #}
{# /* This is pretty simple and straightforward functionality with little variables to be changed */#}
{# /* It is easier to read and debug it in this way. */ #}
{% macro greenplum__udd_insert_unregistered_values(
  relation,
  dictionary_code,
  source_element_code,
  source_element_desc_cols,
  source_system_dk,
  eff_from="valid_from_dttm"
) %}

  {% set hashdiff_expression_use = srv_get_hashdiff_str(['src.source_element_code']) %}

  {% set udd_insert_new_records %}
    with src_rows as (
      select distinct 
            '{{ dictionary_code }}'::varchar(32)                as dictionary_code,
            'NEW'::text                                         as status_code,
            '{{ source_system_dk }}'::text                      as source_system_dk,
            {{ source_element_code }} as source_element_code,
            {{ _udd_concat_fields(source_element_desc_cols) }} as source_element_desc,
            {{ eff_from }}::timestamp(6)  as valid_from_dttm
      from {{ relation }} 
      where {{ source_element_code }} is not null
    )
    insert into grp_meta.un_dictionary_element_map(
      dataflow_id, valid_from_dttm, dictionary_code, element_code, 
      source_element_code, source_element_desc, status_code, source_system_dk,
      priority_ref_source
    )
    select 
      '{{ get_dataflow_id() }}'::text,
      src.valid_from_dttm,
      src.dictionary_code,
      {{ hashdiff_expression_use }}::varchar(32)          as element_code,
      src.source_element_code,
      src.source_element_desc,
      src.status_code,
      src.source_system_dk,
      False as priority_ref_source
    from src_rows src
    left join grp_meta.un_dictionary_element_map mp
      on src.source_element_code = mp.source_element_code
        and mp.dictionary_code = src.dictionary_code
    where mp.source_element_code is null
  {% endset %}

  {% do run_query(udd_insert_new_records) %}

{% endmacro %}

{% macro greenplum__udd_insert_unregistered_array(input_sources_array) %}

  {% do greenplum__udd_validate_insert_unregistered_array_params(input_sources_array) %}

  {% for source_settings in input_sources_array %}
    {% do greenplum__udd_insert_unregistered_values(
      source_settings.get('relation'),   
      source_settings.get('dictionary_code'),   
      source_settings.get('source_element_code'),   
      source_settings.get('source_element_desc_cols'),   
      source_settings.get('source_system_dk')    
    ) %}
  {% endfor %}
{% endmacro %}

{% macro greenplum__udd_validate_insert_unregistered_array_params(input_sources_array) %}

  {% if input_sources_array is not iterable %}
    {% do adapter.raise_compiler_error("UDD ERROR: UDD check unregistered values: Input is not an array.") %}    
  {% endif %}

  {% for source_settings in input_sources_array %}

    {% if source_settings.get('relation') == None
        or source_settings.get('dictionary_code') == None
        or source_settings.get('source_element_code') == None
        or source_settings.get('source_element_desc_cols') == None
        or source_settings.get('source_system_dk') == None
    %}
      {% do adapter.raise_compiler_error("UDD ERROR: UDD check unregistered values: Some of the obligatory parameters were not passed to macro: <relation>,<dictionary_code>,<source_element_code>,<source_element_desc_cols>,<source_system_dk>.") %}    
      
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro greenplum__udd_dictionary_element_update(dictionary_code) %}

  {% set update_un_dictionary_element %}
    delete from grp_meta.un_dictionary_element
    where dictionary_code = '{{ dictionary_code }}';

    with data_prep as (
      select distinct
        dictionary_code,
        element_code,
        source_element_desc
      from grp_meta.un_dictionary_element_map_last_rec
      where status_code != 'OBS'
        and dictionary_code = '{{ dictionary_code }}'
        and priority_ref_source = true
    )
    insert into grp_meta.un_dictionary_element(dataflow_dttm, dataflow_id, dictionary_code, element_code, element_desc)
    select
      now()::timestamp(0),
      '{{ get_dataflow_id() }}',
      dictionary_code,
      element_code,
      string_agg(source_element_desc, '$') as element_desc
    from data_prep
    group by now(), dictionary_code, element_code
  {% endset %}

  {% do run_query(update_un_dictionary_element) %}

{% endmacro %}

{% macro greenplum__apply_udd_unification(dictionary_code, source_column) %}

  {%- set udd_string -%}
    coalesce(
      (select element_code
      from grp_meta.un_dictionary_element_map_last_rec
      where dictionary_code = '{{dictionary_code}}'
        and source_element_code = {{source_column}} ),
      md5({{source_column}})
    )
  {%- endset -%}
  {%- do return(udd_string) -%}
{% endmacro %}