{% macro _scd2_get_join_conditions(columns) %}
    {% set join_condition %}
        {% for col in columns %}
            t1.{{ col }} = t2.{{ col ~ ' '}}
            {%- if not loop.last -%} and {%- endif -%}
        {% endfor %}
    {% endset %}
    {% do return(join_condition) %}
{% endmacro %}

{% macro _scd2_insert_exception(temp_relation, view_exception, src_columns, target, dataflow_dttm, workflow_id) %}
    create view {{ view_exception }}
    as select {{ src_columns }}
    from {{ temp_relation }}
    where scd2_diff_flg<>0 and scd2_flg_new = 0;
    insert into grp_meta.load_exception
    (loader_nm, target_nm, exception_type_code, exception_json, dataflow_id, dataflow_dttm)
    select 'GP SCD2 Loader' as loader_nm,
           '{{ target }}'as target_nm,
           'NUK' as exception_type_code, 
           row_to_json(x) as exception_txt,
           '{{ workflow_id }}' as dataflow_id, 
           {{ dataflow_dttm }} as dataflow_dttm
    from {{ view_exception }} x ;
{% endmacro %}

{% macro _validate_scd2_params(load_key, granular_version, gen_1970_flg) %}
    {% if load_key is not string or load_key == '' %}
        {% do adapter.raise_compiler_error('Invalid load_key provided.\nExpected not empty string value.') %}
    {% endif %}
    {% if granular_version not in ['second', 'microseconds', 'hour', 'day'] %}
        {% do adapter.raise_compiler_error("Invalid granular version provided: "~ granular_version ~ ".\nExpected one of: 'second', 'microseconds', 'hour', 'day'") %}
    {% endif %}
    {% if gen_1970_flg is not boolean %}
        {% do adapter.raise_compiler_error('Invalid gen_1970_flg provided.\nExpected boolean value.') %}
    {% endif %}
{% endmacro %}

{% macro greenplum__get_scd2_sql(target, source_sql) -%}
    {# Constants #}
    {%- set FIRST_CALENDAR_DATE = '1970-01-01' -%}
    {%- set LAST_CALENDAR_DATE = '5999-01-01' -%}
    {##}
    {# Input parameters #}
    {%- set load_key                = greenplum__get_config_colnames('load_key', config, replace_value='') -%}
    {%- set workflow_id             = get_dataflow_id() -%}
    {%- set granular_version        = config.get('granular_version',         'microseconds' ) -%}
    {%- set eff_from                = greenplum__get_config_colnames('eff_from', config, replace_value='') -%}
    {%- set gen_1970_flg            = config.get('gen_1970_flg',             False) -%}

    {{ _validate_scd2_params(load_key, granular_version, gen_1970_flg) }}
    {##}
    {%- set source_columns    = get_columns_in_relation(source_sql)|map(attribute='name')|reject('in', [eff_from])|list -%}
    {%- set target_columns    = get_columns_in_relation(target)|map(attribute='name')|reject('in', ['valid_from_dttm', 'valid_to_dttm', 'dataflow_dttm', 'dataflow_id'])|list -%}
    {%- set join_columns      = load_key.strip().split() -%}
    {%- set attribute_columns = target_columns|reject('in', join_columns)|list -%}
    {%- set tmp_relation_load = make_temp_relation(this,'_scd2_t') -%}
    {%- set tmp_view_exception = make_temp_relation(this,'_scd2_expt') -%}
    {%- set tmp_tabs_settings_text = 'with (appendonly=true, compresstype=zstd, compresslevel=1, orientation=column)' -%}
    {%- set dataflow_dttm = "date_trunc('sec', current_timestamp) at time zone '0'" -%}
    
    {%- set dml -%}
	  {% if eff_from == '' -%} 
      {# Create first temp table #}
      create temp table {{ tmp_relation_load }} 
        {{ tmp_tabs_settings_text }} as
        (select {{ source_columns|join(', ') }},
               current_timestamp    as valid_from_dttm,
               current_timestamp    as valid_to_dttm,
               0::int2              as scd2_diff_flg,
               0::int2              as scd2_flg_new
        from {{ source_sql }} 
        limit 0)
      distributed by ( {{ join_columns|join(', ') }} );

      {# Load first temp table #} 
      insert into {{ tmp_relation_load }}
        with 
          source as (
            select *, 
                  {{ srv_get_hashdiff_str(attribute_columns) }} as hashdiff_key 
            from {{ source_sql }}
          ),
          target as (
			      select {{ source_columns|join(', ') }}, 
                   valid_from_dttm                               as valid_from_dttm_tgt, 
                  {{ srv_get_hashdiff_str(attribute_columns) }}  as hashdiff_key_tgt
				    from {{ target }} 
            where valid_to_dttm = '{{ LAST_CALENDAR_DATE }}'::date
          ),		
          calc_flg as (
            select t1.{{ source_columns|join(', t1.') }},
                   date_trunc('{{ granular_version }}' ,current_timestamp) at time zone '0'                    			 as valid_from_dttm,
                   case when t2.valid_from_dttm_tgt is null                       									  then 1 else 0 end scd2_flg_new,
                   case when date_trunc('{{ granular_version }}' ,current_timestamp) at time zone '0'  = t2.valid_from_dttm_tgt  then 1 else 0 end scd2_from_eq_tgt_flg,
                   case when hashdiff_key_tgt is distinct from hashdiff_key       									  then 1 else 0 end scd2_diff_flg
            from source t1 left join target t2	 
                   on {{ _scd2_get_join_conditions(join_columns) }})

		    select {{ source_columns|join(',') }},
			         case when scd2_from_eq_tgt_flg = 1  then current_timestamp at time zone '0' else valid_from_dttm end valid_from_dttm,
			         '{{ LAST_CALENDAR_DATE }}'::timestamp at time zone '0'                                                         as valid_to_dttm,
               scd2_diff_flg,
			         scd2_flg_new
		    from  calc_flg
		    where scd2_diff_flg = 1;
		   {# Update target actual version #}
       with 
        min_valid_from as (
          select min(valid_from_dttm) as valid_from_dttm
                , {{ join_columns|join(', ') }}
		      from {{ tmp_relation_load }}
		      where scd2_flg_new = 0 
		      group by {{ join_columns|join(', ') }}
        ) 
	    update {{ target }} t1
		      set valid_to_dttm =  (t2.valid_from_dttm - INTERVAL '1 microseconds')
	            , dataflow_dttm = {{ dataflow_dttm }}
	            , dataflow_id = '{{ workflow_id }}'
	        from min_valid_from t2
	        where {{ _scd2_get_join_conditions(join_columns) }}  
	              and t1.valid_to_dttm::date = '{{ LAST_CALENDAR_DATE }}'::date ;
	  {%- else %};
      {# Create first temp table #}
      create temp table {{ tmp_relation_load }} 
        {{ tmp_tabs_settings_text }} as
        (select {{ source_columns|join(', ') }},
               current_timestamp    as valid_from_dttm,
			         current_timestamp    as valid_to_dttm,
               0::int2              as scd2_diff_flg,
               0::int2              as scd2_flg_new,
               current_timestamp    as min_src_from_dttm
          from {{ source_sql }} 
          limit 0)
      distributed by ( {{ join_columns|join(', ') }} );
  
      {# Load first temp table #}
      insert into {{ tmp_relation_load }}
        with 
          source as (
            select {{ source_columns|join(', ') }}
			             , date_trunc('{{ granular_version }}',coalesce( {{ eff_from }},current_timestamp)) at time zone '0' AS valid_from_dttm
                   , {{ srv_get_hashdiff_str(attribute_columns) }} as src_hashdiff_key 
					         , min(date_trunc('{{ granular_version }}',coalesce( {{ eff_from }},current_timestamp)))  over(partition by {{ join_columns|join(', ') }}) at time zone '0'  as min_src_from_dttm
					         , coalesce(lead (date_trunc('{{ granular_version }}',coalesce( {{ eff_from }},current_timestamp)))  over(partition by {{ join_columns|join(', ') }} order by {{ eff_from }} )  - INTERVAL '1 microseconds','{{ LAST_CALENDAR_DATE }}'):: timestamp at time zone '0' as valid_to_dttm
                from {{ source_sql }}
            ),
          target as (
			      select t2.{{ source_columns|join(', t2.') }}, 
                    t2.valid_from_dttm, 
                    t2.valid_to_dttm, 
                    {{ srv_get_hashdiff_str(attribute_columns, table_alias='t2') }} as tgt_hashdiff_key
				    from source t1 join {{ target }} t2
				      on {{ _scd2_get_join_conditions(join_columns) }}
				      and t1.valid_from_dttm = t1.min_src_from_dttm
				      and (t1.valid_from_dttm <= t2.valid_from_dttm or t1.valid_from_dttm<= t2.valid_to_dttm)
          ),		
          source_x_target as (
             select t1.{{ source_columns|join(', t1.') }}
                   ,t1.valid_from_dttm
                   ,t1.valid_to_dttm
                   ,case when t1.valid_from_dttm = t2.valid_from_dttm and t1.valid_to_dttm = t2.valid_to_dttm and src_hashdiff_key = tgt_hashdiff_key then '0'
                         when t1.valid_from_dttm > t2.valid_from_dttm and src_hashdiff_key = tgt_hashdiff_key then '3'
                         when t1.valid_from_dttm > t2.valid_from_dttm and t1.valid_from_dttm = t1.min_src_from_dttm and src_hashdiff_key <> tgt_hashdiff_key then '2,1'
                         when src_hashdiff_key <> tgt_hashdiff_key  or t2.valid_from_dttm  is null then '1'
                         end scd2_diff_flg
                    ,src_hashdiff_key
                    ,tgt_hashdiff_key
                    {%- for trg_col in source_columns %}
                      ,t2.{{ trg_col }} as tgt_{{ trg_col }}
                    {%- endfor %} 
			              ,t2.valid_from_dttm as tgt_valid_from_dttm
			              ,t2.valid_to_dttm as tgt_valid_to_dttm
			              ,min_src_from_dttm
            from source t1 left join target t2	 
              on {{ _scd2_get_join_conditions(join_columns) }} and
				          (t1.valid_from_dttm between t2.valid_from_dttm  and  t2.valid_to_dttm
				          	or
				          t1.valid_to_dttm between t2.valid_from_dttm  and  t2.valid_to_dttm)  
          ),
          unnest_flg as 
            (select {{ source_columns|join(', ') }}
                    , valid_from_dttm
                    , valid_to_dttm
                    , tgt_{{ source_columns|join(', tgt_') }}
                    , tgt_valid_from_dttm
                    , tgt_valid_to_dttm
                    , unnest(string_to_array(scd2_diff_flg, ',')) AS scd2_diff_flg
                    , min_src_from_dttm
            from source_x_target
            where scd2_diff_flg is not null	)
			
        select 
          {%- for col in source_columns %}
             case when scd2_diff_flg in ('3','0') then tgt_{{ col }} else {{ col }} end  {{ col }},
          {%- endfor %} 
			    case when scd2_diff_flg in ('1','0') then valid_from_dttm else tgt_valid_from_dttm end valid_from_dttm
			    , case when scd2_diff_flg in ('1','3','0') then valid_to_dttm else valid_from_dttm - interval '1 microseconds' end valid_to_dttm
			    , scd2_diff_flg::int2
			    , case when tgt_valid_from_dttm is null then 1 else 0 end scd2_flg_new
			    , min_src_from_dttm
		    from unnest_flg;
      {# Delete target version #}	
      delete from {{ target }} t1
      using {{ tmp_relation_load }} t2
      where  {{ _scd2_get_join_conditions(join_columns) }}
        and t1.valid_to_dttm > t2.min_src_from_dttm
        and scd2_diff_flg <> 0
        and (t2.valid_from_dttm between t1.valid_from_dttm and t1.valid_to_dttm
		        or
            t2.valid_to_dttm between t1.valid_from_dttm and t1.valid_to_dttm
            or
           (t2.valid_from_dttm < t1.valid_from_dttm and t2.valid_from_dttm < t1.valid_to_dttm));
	
    {{ _scd2_insert_exception(tmp_relation_load, tmp_view_exception, src_columns, target, dataflow_dttm, workflow_id) }}

	  {%- endif %} 

    {# Insert target table #}
      insert into {{ target }} (
          {{ source_columns|join(', ') }}
          ,valid_from_dttm
          ,valid_to_dttm
          ,dataflow_dttm
          ,dataflow_id)
        select {{ source_columns|join(', ') }}
              , valid_from_dttm
              , valid_to_dttm
              , {{ dataflow_dttm }}                                   as dataflow_dttm
              , '{{ workflow_id }}'                                   as dataflow_id
        from {{ tmp_relation_load }}
		    where scd2_diff_flg <> 0;

      {# Load first record (optional) #}
        {% if gen_1970_flg %}
            insert into {{ target }} (
              {{ join_columns|join(', ') }}
              ,valid_from_dttm
              ,valid_to_dttm
              ,dataflow_dttm
              ,dataflow_id)
            select {{ join_columns|join(', ') }}
                   , '{{ FIRST_CALENDAR_DATE }}'                     as valid_from_dttm
                   , min(valid_from_dttm) - interval '1 microseconds'       as valid_to_dttm
                   , {{ dataflow_dttm }}                                as dataflow_dttm
                   , '{{ workflow_id }}'                                    as dataflow_id
            from {{ tmp_relation_load }}
            where scd2_flg_new = 1
            group by {{ join_columns|join(', ') }}
            having min(valid_from_dttm) > '{{ FIRST_CALENDAR_DATE }}';
        {% endif %}
                                   
    {%- endset -%}

    {% do return(dml) %}

{% endmacro %}