{% macro greenplum__get_pit_sql(target, source_sql) -%} 

    {%- set pit_sources = config.get('pit_sources', none) -%}
    {%- set pit_columns = config.get('pit_columns', none) -%}
    {%- set load_key = greenplum__get_config_colnames('load_key', config, replace_value='') -%}
    {%- set load_type = config.get('load_type', 'FULL_REWRITE')|upper -%}
    {%- set workflow_id = get_dataflow_id() -%}
    {%- set gen_1970_flg = config.get('gen_1970_flg', False) -%}
    {%- set sql_is_cte = config.get('sql_is_cte', False) -%}
    {%- set granular_version = config.get('granular_version', 'microsecond') -%}
    {%- set collapse_to_date = config.get('collapse_to_date', False) -%}       


    {{ _validate_pit_params(pit_sources,
                            pit_columns,
                            load_key,
                            load_type,
                            gen_1970_flg,
                            sql_is_cte,
                            granular_version) }}

    {%- set dml -%}
    {{ _get_pit_loader_sql(pit_sources,
                           pit_columns,
                           target,
                           load_key,
                           load_type,
                           workflow_id,
                           gen_1970_flg,
                           sql_is_cte,
                           source_sql,
                           granular_version,
                           collapse_to_date) }}
    {%- endset -%}

    {% do return(dml) %}

{% endmacro %}

{% macro _validate_pit_params(pit_sources,
                              pit_columns,
                              load_key,
                              load_type,
                              gen_1970_flg,
                              sql_is_cte,
                              granular_version) -%} 
    
    {% if pit_sources is not sequence or pit_sources is string or pit_sources is mapping %}
        {% do adapter.raise_compiler_error('Invalid pit_sources provided.\nExpected list value.') %}
    {% endif %} 

    {% if pit_columns is not sequence or pit_columns is string or pit_columns is mapping %}
        {% do adapter.raise_compiler_error('Invalid pit_columns provided.\nExpected list value.') %}
    {% endif %} 

    {% if pit_columns|length != pit_sources|length %}
        {% do adapter.raise_compiler_error('Sources count is not equal to pit target columns count.') %}
    {% endif %} 
    
    {% if load_key is not string or load_key == '' %}
        {% do adapter.raise_compiler_error('Invalid load_key provided.\nExpected not empty string value.') %}
    {% endif %} 
    
    {% if load_type not in ['INCREMENTAL', 'RK_REWRITE', 'FULL_REWRITE'] %}
        {% do adapter.raise_compiler_error("Invalid load type provided: "~ load_type ~ ".\nExpected one of: 'INCREMENTAL', 'RK_REWRITE', 'FULL_REWRITE'") %}
    {% endif %} 

    {% if gen_1970_flg is not boolean %}
        {% do adapter.raise_compiler_error('Invalid gen_1970_flg provided.\nExpected boolean value.') %}
    {% endif %} 

    {% if sql_is_cte is not boolean %}
        {% do adapter.raise_compiler_error('Invalid sql_is_cte provided.\nExpected boolean value.') %}
    {% endif %} 

    {% if granular_version not in ['second', 'microsecond', 'hour', 'day'] %}
        {% do adapter.raise_compiler_error("Invalid granular version provided: "~ granular_version ~ ".\nExpected one of: 'second', 'microsecond', 'hour', 'day'") %}
    {% endif %}
{% endmacro %}

{% macro _get_pit_loader_sql(pit_sources,
                             pit_columns,
                             pit_target_table,
                             load_key,
                             load_type,
                             workflow_id,
                             gen_1970_flg,
                             sql_is_cte,
                             source_sql,
                             granular_version,
                             collapse_to_date) -%} 

    {%- set pit_target_schema = pit_target_table.schema -%}
    {%- set pit_target_name = pit_target_table.identifier -%}
    {%- set src_incr_tab_name = 'pit_transform_src_incr_' ~ pit_target_name -%}
    {%- set trg_incr_tab_name = 'pit_transform_trg_incr_' ~ pit_target_name -%}
    {%- set FIRST_CALENDAR_DATE = '1970-01-01' -%}
    {%- set LAST_CALENDAR_DATE = '5999-01-01' -%}
    {%- set tmp_schema = 'pg_temp' -%}
    {%- set tmp_tabs_settings_text = 'with (appendonly=true, compresstype=zstd, compresslevel=1, orientation=column)' -%}
    {%- set load_key_list = load_key.split(',') -%}
    {%- set dataflow_dttm = "date_trunc('sec', current_timestamp) at time zone '0'" -%}

    {%- set create_src_incr_tab_query -%}
{% if not sql_is_cte %}
{{ source_sql }}
{% endif %}
create temporary table {{ src_incr_tab_name }}
{{ tmp_tabs_settings_text }} as
{% if sql_is_cte -%}
{{ source_sql }}
{%- endif %}
select *
  from ({% for src_tab in pit_sources %}
        {%- set srt_tab_index = loop.index -%}
        select {{ load_key }},
               valid_from_dttm{%- if collapse_to_date %}::date{% endif %},
               {%- for trg_col in pit_columns %}
               {% set trg_col_index = loop.index -%}
                   {%- if srt_tab_index == trg_col_index -%}
                       {%- if collapse_to_date -%}
                           max(valid_from_dttm)
                       {%- else -%} 
                           valid_from_dttm
                       {%- endif %}
                   {%- else -%} 
                   null::timestamp without time zone
                   {%- endif %} as {{ trg_col }}
                   {%- if not loop.last %},{% endif %}
               {%- endfor %}
          from {{ src_tab }} 
          {%- if collapse_to_date %} 
          group by {{ load_key }}, valid_from_dttm::date
          {% endif %}
        {%- if not loop.last %}
        union all{% endif %}
        {% endfor -%}) incr
distributed by ({{ load_key }});

create temporary table {{ trg_incr_tab_name }}
{{ tmp_tabs_settings_text }} as
with
sub_union as (
    {% if load_type == 'INCREMENTAL' and gen_1970_flg -%}
    select {{ load_key }},
           coalesce(p.valid_from_dttm,'{{ FIRST_CALENDAR_DATE }}'::timestamp without time zone) as valid_from_dttm,
           p.valid_from_dttm as valid_from_dttm_pit,
           p.valid_to_dttm as valid_to_dttm_pit,
           {%- for trg_col in pit_columns %}
           p.{{ trg_col }} as {{ trg_col }}_pit,
           {%- endfor %}
           {%- for trg_col in pit_columns %}
           null::timestamp without time zone as {{ trg_col }}
           {%- if not loop.last %},{% endif %}
           {%- endfor %}
      from (select {{ load_key }},
                   min(valid_from_dttm) as valid_from_dttm
              from {{ tmp_schema }}.{{ src_incr_tab_name }}
             group by {{ load_key }}) s
      left
      join {{ pit_target_schema }}.{{ pit_target_name }} p
     using ({{ load_key }})
     where p.valid_to_dttm is null
        or p.valid_to_dttm >= s.valid_from_dttm
    union all
    {% elif load_type == 'INCREMENTAL' and not gen_1970_flg -%}
    select {{ load_key }},
           p.valid_from_dttm,
           p.valid_from_dttm as valid_from_dttm_pit,
           p.valid_to_dttm as valid_to_dttm_pit,
           {%- for trg_col in pit_columns %}
           p.{{ trg_col }} as {{ trg_col }}_pit,
           {%- endfor %}
           {%- for trg_col in pit_columns %}
           null::timestamp without time zone as {{ trg_col }}
           {%- if not loop.last %},{% endif %}
           {%- endfor %}
      from (select {{ load_key }},
                   min(valid_from_dttm) as valid_from_dttm
              from {{ tmp_schema }}.{{ src_incr_tab_name }}
             group by {{ load_key }}) s
      join {{ pit_target_schema }}.{{ pit_target_name }} p
     using ({{ load_key }})
     where p.valid_to_dttm >= s.valid_from_dttm
    union all
    {% elif load_type != 'INCREMENTAL' and gen_1970_flg -%}
    select {{ load_key }},
           '{{ FIRST_CALENDAR_DATE }}'::timestamp without time zone as valid_from_dttm,
           null as valid_from_dttm_pit,
           null as valid_to_dttm_pit,
           {%- for trg_col in pit_columns %}
           null::timestamp without time zone as {{ trg_col }}_pit,
           {%- endfor %}
           {%- for trg_col in pit_columns %}
           null::timestamp without time zone as {{ trg_col }}
           {%- if not loop.last %},{% endif %}
           {%- endfor %}
      from {{ tmp_schema }}.{{ src_incr_tab_name }}
     group by {{ load_key }}
    union all
    {% endif -%}
    select {{ load_key }},
           valid_from_dttm,
           null as valid_from_dttm_pit,
           null as valid_to_dttm_pit,
           {%- for trg_col in pit_columns %}
           null as {{ trg_col }}_pit,
           {%- endfor %}
           {%- for trg_col in pit_columns %}
           {{ trg_col }}
           {%- if not loop.last %},{% endif %}
           {%- endfor %}
      from {{ tmp_schema }}.{{ src_incr_tab_name }}
),
sub as (
    select {{ load_key }},
           valid_from_dttm,
           max(valid_from_dttm_pit)::timestamp without time zone as valid_from_dttm_pit,
           max(valid_to_dttm_pit)::timestamp without time zone as valid_to_dttm_pit,
           {%- for trg_col in pit_columns %}
           max(coalesce({{ trg_col }}, {{ trg_col }}_pit))::timestamp without time zone as {{ trg_col }},
           max({{ trg_col }}_pit)::timestamp without time zone as {{ trg_col }}_pit
           {%- if not loop.last %},{% endif %}
           {%- endfor %}
      from sub_union
     group by {{ load_key }}, valid_from_dttm
),
main as (
    select {{ load_key }},
           valid_from_dttm,
           valid_from_dttm_pit,
           lead(valid_from_dttm - interval '1 {{ granular_version }}',1,'{{ LAST_CALENDAR_DATE }}'::timestamp without time zone) over (partition by {{ load_key }} order by valid_from_dttm) as valid_to_dttm,
           valid_to_dttm_pit as valid_to_dttm_pit,
           {%- for trg_col in pit_columns %}
           max({{ trg_col }}) over (partition by {{ load_key }} order by valid_from_dttm rows between unbounded preceding and current row) as {{ trg_col }},
           {{ trg_col }}_pit
           {%- if not loop.last %},{% endif %}
           {%- endfor %}
      from sub
)
select {{ load_key }},
       valid_from_dttm,
       valid_to_dttm,
       {%- for trg_col in pit_columns %}
       {{ trg_col }},
       {%- endfor %}
       case
           when valid_from_dttm_pit is null
               then 'I'
           when valid_to_dttm_pit <> valid_to_dttm
             {%- for trg_col in pit_columns %}
             or {{ trg_col }}_pit <> {{ trg_col }}
             {%- endfor %}
               then 'U'
       end as code_change_type
  from main
 where valid_from_dttm_pit is null
    or valid_to_dttm_pit <> valid_to_dttm
    {%- for trg_col in pit_columns %}
    or {{ trg_col }}_pit <> {{ trg_col }}
    {%- endfor %}
distributed by ({{ load_key }});

{% if load_type == 'INCREMENTAL' -%}
update {{ pit_target_schema }}.{{ pit_target_name }} trg
   set valid_to_dttm = src.valid_to_dttm,
       dataflow_id = '{{ workflow_id }}'::text,
       dataflow_dttm = {{ dataflow_dttm }},
       {%- for trg_col in pit_columns %}
       {{ trg_col }} = src.{{ trg_col }}{% if not loop.last %},{% endif %}
       {%- endfor %}
  from {{ tmp_schema }}.{{ trg_incr_tab_name }} src
 where trg.valid_from_dttm = src.valid_from_dttm
   {%- for key_col in load_key_list %}
   and trg.{{ key_col }} = src.{{ key_col }} 
   {%- endfor %}
   and code_change_type= 'U';
{% elif load_type == 'RK_REWRITE' -%}
delete from {{ pit_target_schema }}.{{ pit_target_name }} trg
 using {{ tmp_schema }}.{{ trg_incr_tab_name }} src
 where 1 = 1
   {%- for key_col in load_key_list %}
   and trg.{{ key_col }} = src.{{ key_col }} 
   {%- endfor %};
{% elif load_type == 'FULL_REWRITE' -%}
truncate table {{ pit_target_schema }}.{{ pit_target_name }};
{%- endif %}

insert into {{ pit_target_schema }}.{{ pit_target_name }}
    (dataflow_id,
     dataflow_dttm,
     {{ load_key }},
     valid_from_dttm,
     valid_to_dttm,
     {%- for trg_col in pit_columns %}
     {{ trg_col }}{% if not loop.last %},{% endif %}
     {%- endfor -%}
     )
select '{{ workflow_id }}'::text as dataflow_id,
       {{ dataflow_dttm }} as dataflow_dttm,
       {{ load_key }},
       valid_from_dttm,
       valid_to_dttm,
       {%- for trg_col in pit_columns %}
       {{ trg_col }}{% if not loop.last %},{% endif %}
       {%- endfor %}
  from {{ tmp_schema }}.{{ trg_incr_tab_name }}
 where code_change_type= 'I';


    {% endset -%}
    {% do return(create_src_incr_tab_query) %}
{% endmacro %}