{% macro _sat_get_join_conditions(columns) %}
    {% set join_condition %}
        {% for col in columns %}
            t1.{{ col }} = t2.{{ col ~ ' '}}
            {%- if not loop.last -%} and {%- endif -%}
        {% endfor %}
    {% endset %}
    {% do return(join_condition) %}
{% endmacro %}

{%- macro _sat_get_temp_table_name(source_sql, idx) -%}
    {{ source_sql.schema ~ '.' ~ source_sql.identifier ~ '_t' ~ idx }}
{%- endmacro -%}

{%- macro _sat_drop_temp_tables(source_sql) -%}
    {%- set drop_temp_tables -%}
        {%- for i in [1, 2, 3, 4] %}
            drop {%- if i == 3 %} view {%- else %} table {%- endif %} if exists {{ _sat_get_temp_table_name(source_sql, i) }} cascade;
        {%- endfor %}
    {%- endset -%}
    {%- do return(drop_temp_tables) -%}
{%- endmacro -%}

{# deprecated for now - nowhere to use #}
{% macro _sat_target_table_query(target, join_columns, source_columns, attribute_columns, delete_flg) %}
    {%- set  target_table_query -%}
        (select distinct on ({{ join_columns|join(', ') }}) 
                {{ source_columns|join(', ') }}, 
                valid_from_dttm                               as valid_from_dttm_tgt, 
                {{ srv_get_hashdiff_str(attribute_columns) }} as hashdiff_key_tgt
                {% if delete_flg -%}
                ,delete_flg as delete_flg_tgt
                {%- endif %}
           from {{ target }} 
          order by {{ join_columns|join(', ') }}, valid_from_dttm desc) t2
    {%- endset -%}
    {%- do return(target_table_query) -%}
{% endmacro %}

{% macro _validate_sat_params(load_key, preprocess_flg, version_type_flg, delete_flg, gen_1970_flg, save_exception, prematerialize_keys) %}
    {% if load_key is not string or load_key == '' %}
        {% do adapter.raise_compiler_error('Invalid load_key provided.\nExpected not empty string value.') %}
    {% endif %}
    {% if preprocess_flg is not boolean %}
        {% do adapter.raise_compiler_error('Invalid preprocess_flg provided.\nExpected boolean value.') %}
    {% endif %}
    {% if version_type_flg is not boolean %}
        {% do adapter.raise_compiler_error('Invalid version_type_flg provided.\nExpected boolean value.') %}
    {% endif %}
    {% if delete_flg is not boolean %}
        {% do adapter.raise_compiler_error('Invalid delete_flg provided.\nExpected boolean value.') %}
    {% endif %}
    {% if gen_1970_flg is not boolean %}
        {% do adapter.raise_compiler_error('Invalid gen_1970_flg provided.\nExpected boolean value.') %}
    {% endif %}
    {% if save_exception is not boolean %}
        {% do adapter.raise_compiler_error('Invalid save_exception provided.\nExpected boolean value.') %}
    {% endif %}
    {% if prematerialize_keys is not boolean %}
        {% do adapter.raise_compiler_error('Invalid prematerialize_keys provided.\nExpected boolean value.') %}
    {% endif %}
{% endmacro %}

{% macro greenplum__get_sat_sql(target, source_sql) -%}
    {# Constants #}
    {%- set FIRST_CALENDAR_DATE = '1970-01-01' -%}
    {##}
    {# Input parameters #}
    {%- set load_key                = greenplum__get_config_colnames('load_key', config, replace_value='') -%}
    {%- set workflow_id             = get_dataflow_id() -%}
    {%- set preprocess_flg          = config.get('preprocess_flg',           True ) -%}
    {%- set version_type_flg        = config.get('version_type_flg',         True ) -%}
    {%- set eff_from                = greenplum__get_config_colnames('eff_from', config, replace_value='') -%}
    {%- set delete_flg              = config.get('delete_flg',               True ) -%}
    {%- set gen_1970_flg            = config.get('gen_1970_flg',             False) -%}
    {%- set save_exception          = config.get('save_exception',           True ) -%}
    {%- set prematerialize_keys     = config.get('prematerialize_keys',      False) -%}

    {{ _validate_sat_params(load_key, preprocess_flg, version_type_flg, delete_flg, gen_1970_flg, save_exception, prematerialize_keys) }}
    {##}
    {%- set delete_flag_list  = ['"delete_flg"'] if delete_flg else [] -%}
    {%- set source_columns    = get_columns_in_relation(source_sql)|map(attribute='name')|reject('in', delete_flag_list + [eff_from])|list -%}
    {%- set target_columns    = get_columns_in_relation(target)|map(attribute='name')|reject('in', delete_flag_list + ['"valid_from_dttm"', '"dataflow_dttm"', '"dataflow_id"'])|list -%}
    {%- set join_columns      = load_key.strip().split(",") -%}
    {%- set attribute_columns = target_columns|reject('in', join_columns)|list -%}
    {##}
    {%- set cast_valid_from    = '::date' if version_type_flg else '' -%}
    {%- set value_eff_from     = 'current_timestamp' ~ cast_valid_from if eff_from == '' else 'coalesce(t1.' ~ eff_from ~ ',current_timestamp)' ~ cast_valid_from -%}
    {# set target_table_query = _sat_target_table_query(target, join_columns, source_columns, attribute_columns, delete_flg) #}
    {%- set tmp_tabs_settings_text = 'with (appendonly=true, compresstype=zstd, compresslevel=1, orientation=column)' -%}

    {%- set dml -%}
        {{ _sat_drop_temp_tables(source_sql) }}
        {# Create first temp table #}
        create table {{ _sat_get_temp_table_name(source_sql, 1) }} 
        {{ tmp_tabs_settings_text }} as
        select {{ source_columns|join(', ') }},
               current_timestamp    as valid_from_dttm,
               null::text           as hashdiff_key,
               null::text           as scd2_hashdiff_key_tgt,
               {%- if delete_flg -%}
               0 ::int2             as delete_flg, 
               0 ::int2             as delete_flg_tgt,
               {%- endif -%} 
               0::int2              as scd2_flg_new,
               0::int2              as scd2_loader_from_flg,
               0::int2              as scd2_diff_flg,
               0::int4              as scd2_rn
          from {{ source_sql }} limit 0
        distributed by ( {{ join_columns|join(', ') }} );
        {##}  

        {# Load first temp table #}
        {% if prematerialize_keys %}
            create table {{ _sat_get_temp_table_name(source_sql, 4) }} 
            {{ tmp_tabs_settings_text }} as 
        {% else %}
          insert into {{ _sat_get_temp_table_name(source_sql, 1) }}
          with source as (
        {% endif %}

              select distinct on (t1.{{ join_columns|join(', t1.') }})
                t1.{{ join_columns|join(', t1.') }},
                t1.valid_from_dttm as valid_from_dttm_tgt,
                {{ srv_get_hashdiff_str(attribute_columns, 't1') }} as hashdiff_key_tgt
                {% if delete_flg -%}
                ,delete_flg as delete_flg_tgt
                {%- endif %}
              from {{ target }} t1
              inner join (
                  select distinct {{ join_columns|join(', ') }} from {{ source_sql }}
              ) t2
                on {{ _sat_get_join_conditions(join_columns) }}
              order by t1.{{ join_columns|join(', t1.') }}, t1.valid_from_dttm desc

          {% if prematerialize_keys %} 
            distributed by ( {{ join_columns|join(', ') }} );
            insert into {{ _sat_get_temp_table_name(source_sql, 1) }}
          {% else %}
          ) 
          {% endif %}
          
          select t1.{{ source_columns|join(', t1.') }},
                 {{ value_eff_from }}                     as valid_from_dttm,
                 {{ srv_get_hashdiff_str(attribute_columns, 't1') }} as hashdiff_key,
                 t2.hashdiff_key_tgt                      as scd2_hashdiff_key_tgt, 
                 {%- if delete_flg %}
                 0 ::int2 as delete_flg, 
                 delete_flg_tgt,
                 {%- endif %}
                 case when t2.valid_from_dttm_tgt is null                       then 1 else 0 end scd2_flg_new,
                 case when {{ value_eff_from }} = t2.valid_from_dttm_tgt        then 1 else 0 end scd2_loader_from_flg,
                 case when hashdiff_key_tgt is distinct from {{ srv_get_hashdiff_str(attribute_columns, 't1') }}  
                        {% if delete_flg -%} or delete_flg_tgt = 1 {%- endif %} then 1 else 0 end scd2_diff_flg,
                 row_number () over (partition by t1.{{ join_columns|join(', t1.') }}, {{ value_eff_from }}) as scd2_rn
            from {{ source_sql }} t1 
            left join {% if prematerialize_keys %} {{ _sat_get_temp_table_name(source_sql, 4) }} {% else %} source {% endif %} t2
              on {{ _sat_get_join_conditions(join_columns) }} 
            where (t2.valid_from_dttm_tgt is null or {{ value_eff_from }} >= t2.valid_from_dttm_tgt) ;
        analyze {{ _sat_get_temp_table_name(source_sql, 1) }};
        {##}
        {# Saving exception (optional) #}
        {% if save_exception %}
            create or replace view {{ _sat_get_temp_table_name(source_sql, 3) }} as 
              select {{ source_columns|join(', ') }}, 
                     valid_from_dttm,
                     case 
                       when scd2_loader_from_flg = 1 
                        and scd2_diff_flg = 1 then 1 
                       else 0 
                     end scd2_exeption_flg
                from {{ _sat_get_temp_table_name(source_sql, 1) }}
               where scd2_rn > 1 or (scd2_loader_from_flg = 1 and scd2_diff_flg = 1);
            
            insert into grp_meta.load_exception (loader_nm, target_nm, exception_type_code, exception_json, dataflow_id, dataflow_dttm)
              select 'GP SAT Loader'               as loader_nm,
                     '{{ target }}'                as target_nm,
                     case 
                       when scd2_exeption_flg = 1
                       then 'KAE'
                       else 'NUK' 
                     end                           as exception_type_code, 
                     row_to_json(x)                as exception_txt,
                     '{{ workflow_id }}'           as dataflow_id,
                     now()                         as dataflow_dttm
                from {{ _sat_get_temp_table_name(source_sql, 3) }} x;
        {% endif %}
        {##}
        {# Process delete rows (optional) #}
        {% if delete_flg %}
            insert into {{ _sat_get_temp_table_name(source_sql, 1) }} (
              {{ source_columns|join(', ') }},
              valid_from_dttm,
              hashdiff_key,
              scd2_hashdiff_key_tgt,
              delete_flg_tgt,
              delete_flg,
              scd2_flg_new,
              scd2_loader_from_flg,
              scd2_diff_flg,
              scd2_rn
            )
            select t2.{{ source_columns|join(', t2.') }},
                   current_timestamp{{ cast_valid_from }} as valid_from_dttm, 
                   hashdiff_key_tgt,
                   null as scd2_hashdiff_key_tgt,
                   null as delete_flg_tgt,
                   1 as delete_flg,
                   0 as scd2_flg_new,
                   0 as scd2_loader_from_flg,
                   1 as scd2_diff_flg,
                   1 as scd2_rn
             from {{ target_table_query }} left join
                  (select distinct 
                          {{ join_columns|join(', ') }} 
                     from {{ _sat_get_temp_table_name(source_sql, 1) }}
                    where scd2_flg_new = 0
                  ) t1
               on {{ _sat_get_join_conditions(join_columns) }}
            where t1.{{ join_columns|join(' is null and t1.') }} is null
              and t2.delete_flg_tgt = 0;
        {% endif %}
        {##}
        {# Rollup process (optional) #}
        {% if preprocess_flg %}
            create table {{ _sat_get_temp_table_name(source_sql, 2) }}
              {{ tmp_tabs_settings_text }} as
              select {{ source_columns|join(', ') }},
                     valid_from_dttm,
                     scd2_flg_new,
                     {%- if delete_flg %}
                     delete_flg,
                     {%- endif %}
                     hashdiff_key, 
                     0::int2 as scd2_loader_from_flg, 
                     1::int2 as scd2_diff_flg, 
                     1::int2 as scd2_rn
                from {{ _sat_get_temp_table_name(source_sql, 1) }} limit 0
              distributed by ( {{ join_columns|join(', ') }} );

            insert into {{ _sat_get_temp_table_name(source_sql, 2) }}
              select distinct on ( {{ join_columns|join(', ') }}, valid_from_dttm )
                     {{ source_columns|join(', ') }},
                     valid_from_dttm,
                     scd2_flg_new,
                     {%- if delete_flg %}
                     delete_flg,
                     {%- endif %}
                     hashdiff_key,
                     0::int2 as scd2_loader_from_flg,
                     1::int2 as scd2_diff_flg,
                     1::int2 as scd2_rn
                from (select {{ source_columns|join(', ') }},
                             valid_from_dttm,
                             scd2_flg_new, 
                             {%- if delete_flg %}
                             delete_flg,
                             {%- endif %}
                             hashdiff_key,
                             case 
                               when hashdiff_key = coalesce(
                                                     lag(hashdiff_key)
                                                       over (partition by {{ join_columns|join(', ') }} order by valid_from_dttm),
                                                     scd2_hashdiff_key_tgt
                                                   )
                                {%- if delete_flg %}
                                and delete_flg is not distinct from coalesce(
                                                                      lag(delete_flg) 
                                                                        over (partition by {{ join_columns|join(', ') }} order by valid_from_dttm),
                                                                      delete_flg_tgt
                                                                    )
                                {%- endif %}
                               then 1
                               else 0 
                             end scd2_del_flg
                        from {{ _sat_get_temp_table_name(source_sql, 1) }}
                       where scd2_loader_from_flg = 0) t1
               where scd2_del_flg=0;
        {% endif %}
        {##}
        {# Load target table #}
        insert into {{ target }} (
          {{ source_columns|join(', ') }},
          valid_from_dttm,
          dataflow_dttm,
          {%- if delete_flg %}
          delete_flg,
          {%- endif %}
          dataflow_id
        )
        select {{ source_columns|join(', ') }}, 
               valid_from_dttm,
               current_timestamp::timestamp as dataflow_dttm,
               {%- if delete_flg %}
               delete_flg,
               {%- endif %}
               '{{ workflow_id }}' as dataflow_id
          from {{ _sat_get_temp_table_name(source_sql, 2 if preprocess_flg else 1) }}
         {% if not preprocess_flg -%} 
         where scd2_loader_from_flg = 0
           and scd2_diff_flg = 1
           and scd2_rn = 1
         {%- endif %};
        {##}
        {# Load first record (optional) #}
        {% if gen_1970_flg %}
            insert into {{ target }} (
              {{ join_columns|join(', ') }},
              valid_from_dttm,
              dataflow_dttm,
              dataflow_id
            )
            select {{ join_columns|join(', ') }},
                   '{{ FIRST_CALENDAR_DATE }}'::date  as valid_from_dttm,
                   current_timestamp::timestamp       as dataflow_dttm,
                   '{{ workflow_id }}'                as dataflow_id
              from {{ _sat_get_temp_table_name(source_sql, 2 if preprocess_flg else 1) }}
             where scd2_flg_new = 1
             group by {{ join_columns|join(', ') }}
            having min(valid_from_dttm) > '{{ FIRST_CALENDAR_DATE }}'::date;
        {% endif %}
        {##}
        {{ _sat_drop_temp_tables(source_sql) }}                                   
    {%- endset -%}

    {% do return(dml) %}

{% endmacro %}