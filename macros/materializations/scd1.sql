{% macro get_dataflow_id() -%}
  {%set dataflow_id = 'dbt__' ~ invocation_id%}
  {{ return (dataflow_id) }}
{%- endmacro %}

{% macro _validate_scd1_params(source,
                               target,
                               load_key,
                               load_type,
                               distributed_key,
                               partition_comparison_operator,
                               reaction_to_double_code,
                               partition_column_nm,
                               fields_for_compare,
                               reaction_to_null_flg) -%}


    {%- set src_columns_list = get_columns_in_relation(source) | map(attribute='name') | reject('in', ['"dataflow_dttm"', '"dataflow_id"']) | sort | list -%}
    {%- set tgt_columns_list = get_columns_in_relation(target) | map(attribute='name') | reject('in', ['"dataflow_dttm"', '"dataflow_id"']) | sort | list -%}
    {%- set load_key_list = load_key.split(',') -%}
    {%- set distributed_key_list = distributed_key.split(',') -%}
    {%- set fields_for_compare_list = fields_for_compare.split(',') | reject ('in' , '') | list -%}

    {% if src_columns_list != tgt_columns_list %}
        {% do adapter.raise_compiler_error('Src column names not equal target column names.\n {}:{} \n {}:{}'.format(source|string,src_columns_list|string, target|string,tgt_columns_list|string )) %}
    {% endif %}

    {% if load_key_list | length < 1 or load_key_list | reject ('in' , src_columns_list + ['dataflow_dttm', 'dataflow_id']) | list %}
        {% do adapter.raise_compiler_error('Invalid load_key provided.\nExpected not empty string value.') %}
    {% endif %}

    {% if distributed_key_list | length < 1 or distributed_key_list | reject ('in' , src_columns_list + ['dataflow_dttm', 'dataflow_id', '"randomly"', '"RANDOMLY"']) | list %}
        {% do adapter.raise_compiler_error('Invalid distributed_key provided.\nExpected not empty string value.') %}
    {% endif %}

    {% if fields_for_compare_list | length >= 1 and fields_for_compare_list | reject ('in' , src_columns_list + ['dataflow_dttm', 'dataflow_id']) | list %}
        {% do adapter.raise_compiler_error('Invalid fields_for_compare provided.') %}
    {% endif %}

    {% if load_type not in ['A', 'R', 'U'] %}
        {% do adapter.raise_compiler_error("Invalid load type provided: "~ load_type ~ ".\nExpected one of: 'R', 'U', 'A'") %}
    {% endif %}

    {% if reaction_to_double_code not in [1, 2, 3, 4] %}
        {% do adapter.raise_compiler_error("Invalid reaction type provided: "~ reaction_to_double_code ~ ".\nExpected one of: 1, 2, 3, 4") %}
    {% endif %}

    {% if partition_comparison_operator not in ['=', '>='] %}
        {% do adapter.raise_compiler_error("Invalid partition_comparison_operator provided.\nExpected one of: '=', '>=' .") %}
    {% endif %}

    {% if partition_column_nm != '' and partition_column_nm not in src_columns_list %}
        {% do adapter.raise_compiler_error("Invalid partition_column_nm provided.") %}
    {% endif %}

    {% if reaction_to_null_flg is not boolean %}
        {% do adapter.raise_compiler_error('Invalid reaction_to_null_flg provided.\nExpected boolean value.') %}
    {% endif %}

{% endmacro %}

{# /* Calculate partition_cond */ #}
{% macro _scd1__get_partition_cond(source, partition_column_nm, src_columns_list, partition_comparison_operator) %}

    {%- set SPACE = ' ' -%}
    {%- set partition_cond = '' -%}

    {%- if partition_column_nm != '' -%}

        {%- set partition_column_type -%}
            {%- for n in src_columns_list -%}
                {%- if n.name == partition_column_nm -%}
                    {{ n.data_type }}
                {%- endif -%}
            {%- endfor -%}
        {%- endset -%}

        {%- if partition_comparison_operator == '=' -%}

            {%- call statement('get_partitions', fetch_result=True) -%}
                select distinct {{ partition_column_nm }} dt
                from {{ source }}
                order by 1
            {%- endcall -%}
            {%- set part_list = load_result('get_partitions').table -%}
            {%- set partition_cond -%}
                and (
                {%- for part in part_list -%}
                    t2.{{ partition_column_nm }} = '{{ part[0] }}'::{{ partition_column_type }}
                    {%- if not loop.last %}{{ SPACE }}or{{ SPACE }}{% endif %}
                {%- endfor -%}
                )
            {%- endset -%}

        {%- elif partition_comparison_operator == '>='-%}

            {%- call statement('get_partitions', fetch_result=True) -%}
                select min( {{ partition_column_nm }} ) dt
                from {{ source }}
            {%- endcall -%}
            {%- set part_list = load_result('get_partitions').table -%}
            {%- set partition_cond -%}
                and (
                {%- for part in part_list -%}
                    t2.{{ partition_column_nm }} >= '{{ part[0] }}'::{{ partition_column_type }}
                {%- endfor -%}
                )
            {%- endset -%}

        {% else %}
            {%- do adapter.raise_compiler_error('SCD1 Load ERROR: partition_comparison_operator type unknow: ' ~ partition_comparison_operator ~ '.') -%}
        {%- endif -%}
    {%- endif -%}

    {{ return(partition_cond) }}
{% endmacro %}

{% macro _scd1__get_join_cond(join_keys) %}

    {%- set SPACE = ' ' -%}

    {%- set join_cond_str -%}
        {%- for key in join_keys -%}
            t1.{{ key }} = t2.{{ key }}
            {%- if not loop.last -%} {{ SPACE }}and{{ SPACE }} {%- endif -%}
        {%- endfor -%}
    {%- endset -%}

    {{ return(join_cond_str) }}
{% endmacro %}

{%- macro greenplum__get_scd1_sql(target, source, workflow_id=None, colname_delimiter='') -%}

    {# Constants #}
    {%- set SPACE = ' ' -%}
    {%- set compress_settings_text = 'with (appendonly=true, compresstype=zstd, compresslevel=1, orientation=column)' -%}
    {%- set dataflow_dttm = "date_trunc('sec', current_timestamp) at time zone '0'" -%}
    

    {# /* Get params from config: */ #}
    {%- set src_columns_list = get_columns_in_relation(source) -%}
    {%- set load_key = greenplum__get_config_colnames('load_key', config, replace_value='') -%}
    {%- set reaction_to_null_flg = config.get('reaction_to_null_flg', True ) -%}
    {%- set load_type = config.get('load_type', 'U') |trim |string -%}
    {%- set distributed_key = greenplum__get_config_colnames('distributed_by', config, replace_value=load_key) -%}
    {%- set fields_for_compare = greenplum__get_config_colnames('fields_for_compare', config, replace_value='') -%}
    {%- set partition_column_nm = greenplum__get_config_colnames('partition_column_nm', config, replace_value='') -%}
    {%- set partition_comparison_operator = config.get('partition_comparison_operator', '=') |trim |string -%}
    {%- set reaction_to_double_code = config.get('reaction_to_double_code', 1) -%}

    {{ _validate_scd1_params(  source,
                               target,
                               load_key,
                               load_type,
                               distributed_key,
                               partition_comparison_operator,
                               reaction_to_double_code,
                               partition_column_nm,
                               fields_for_compare,
                               reaction_to_null_flg) }}


    {# Calculation of supporting variables: #}
    {% if workflow_id == None %} {%- set workflow_id = get_dataflow_id() -%} {% endif %}

    {% set src_columns = src_columns_list |map(attribute='name') | join(', ') %}

    {%- set join_keys = load_key.split(',') -%}
    {%- set tmp_relation_load = make_temp_relation(this,'_tdb') -%}
    {%- set tmp_relation_load_view = make_temp_relation(this,'_vfe') -%}
    {%- set tmp_relation_load_final = make_temp_relation(this,'_tdi') -%}
    {%- set table_for_load = source -%}

    {%- if reaction_to_null_flg -%}
        {%- set sign_value = ' = '-%}
    {%- else -%}
        {%- set sign_value = ' IS NOT DISTINCT FROM '-%}
    {%- endif -%}

    {%- call statement('check_double', fetch_result=True) -%}
        select 1 from {{ source }} group by {{ load_key }} having count(*)>1 limit 1
    {%- endcall -%}
    {%- set check_result = load_result('check_double')['data']|list -%}

    {%- if fields_for_compare == '' -%}
        {%- set field_f_compare_columns = src_columns_list -%}
    {%- else -%}
        {%- set field_f_compare_columns = fields_for_compare.split(',') -%}
    {%- endif -%}

    {%- set join_cond_str = _scd1__get_join_cond(join_keys) -%}
    {% set partition_cond = _scd1__get_partition_cond(source, partition_column_nm, src_columns_list, partition_comparison_operator) %}
    
    {%- set dml -%}
        {% if load_type == 'R' %}

            truncate {{ target }};
            insert into {{ target }}
            (dataflow_dttm
            ,dataflow_id
            ,{{ src_columns }}
            )
            select
            {{ dataflow_dttm }}
            , '{{ workflow_id }}'
            ,{{ src_columns }}
            from {{ source }};

        {% elif load_type in ('U','A') %}

            {% if check_result %}

                    create temporary table {{ tmp_relation_load }}
                    {{ compress_settings_text }}
                    as select {{ src_columns }}, null::int2 as rn_for_loader_flg, null::int2 as except_for_loader_flg
                    from {{ source }} limit 0
                    {{ distributed_by(distributed_key) }} ;
                    create view {{ tmp_relation_load_view }}
                    as select {{ src_columns }}
                    from {{ tmp_relation_load }}
                    where except_for_loader_flg>1;

                {% if reaction_to_double_code == 1 %}

                    {%- do adapter.raise_compiler_error("The source table contains duplicates by key") -%}

                {% elif reaction_to_double_code == 2 %}

                        insert into {{ tmp_relation_load }}
                        select {{ src_columns }},
                                row_number() over(partition by {{ load_key }}),
                                count(1) over(partition by {{ load_key }})
                        from {{ source }} ;

                    {%- set where_double_flg = ' and rn_for_loader_flg = 1' -%}

                {% elif reaction_to_double_code == 3 %}

                        insert into {{ tmp_relation_load }}
                        select {{ src_columns }},
                                count(1) over(partition by {{ load_key }}),
                                count(1) over(partition by {{ load_key }})
                        from {{ source }} ;

                    {%- set where_double_flg = ' and except_for_loader_flg = 1 ' -%}

                {% elif reaction_to_double_code == 4 %}
                    {# Do nothing with duplicates by key #}

                        insert into {{ tmp_relation_load }}
                        select {{ src_columns }}, 1, 1
                        from {{ source }} ;

                    {%- set where_double_flg = '' -%}
                {% endif %}

                    insert into grp_meta.load_exception
                    (loader_nm, target_nm, exception_type_code, exception_json, dataflow_id, dataflow_dttm)
                    select 'GP SCD1 Loader' as loader_nm,
                           '{{ target }}'as target_nm,
                           'NUK' as exception_type_code, 
                           row_to_json(x) as exception_txt,
                           '{{ workflow_id }}' as dataflow_id, 
                           {{ dataflow_dttm }} as dataflow_dttm
                    from {{ tmp_relation_load_view }} x ;

                {% set table_for_load = tmp_relation_load %}

            {% endif %}

            {% if load_type == 'A' %}

                insert into {{ target }} ({{ src_columns }},dataflow_dttm,dataflow_id )
                select t1.{{ src_columns_list|map(attribute= 'name')|join(', t1.') }},
                        {{ dataflow_dttm }} as dataflow_dttm,
                        '{{ workflow_id }}' as dataflow_id
                from {{ table_for_load }} t1 left join {{ target }} t2
                on {{ join_cond_str }}
                    {{ partition_cond }}
                where t2.{{ join_keys[0] }} is null
                    {{ where_double_flg }};

            {% elif load_type == 'U' %}

                    create temporary table {{ tmp_relation_load_final }}
                    {{ compress_settings_text }}
                    as select t1.{{ src_columns_list|map(attribute= 'name')|join(', t1.') }}
                            , case when t2.{{ join_keys[0] }} is null then 2
                                   when md5( coalesce(t1.{{ field_f_compare_columns|map(attribute= 'name')|join("::varchar , '') ||'_'|| \ncoalesce(t1.") }}::varchar, '')) <>
                                        md5( coalesce(t2.{{ field_f_compare_columns|map(attribute= 'name')|join("::varchar , '') ||'_'|| \ncoalesce(t2.") }}::varchar, ''))
                                   then 1
                                   else 0 end flg_to_scd1_tmp
                    from {{ table_for_load }} t1 
                    left join {{ target }} t2 
                        on {{ join_cond_str }} {{ partition_cond }}
                    where 1=1 {{ where_double_flg }}
                    {{ distributed_by(distributed_key) }} ;

                    delete from {{ target }} as t2
                    using {{ tmp_relation_load_final }} as t1
                    where {{ join_cond_str }}
                        and t1.flg_to_scd1_tmp in (1) 
                    {{ partition_cond }};

                    insert into {{ target }} (
                        {{ src_columns }}
                        ,dataflow_dttm
                        ,dataflow_id
                    )
                    select {{ src_columns }}
                           , {{ dataflow_dttm }} as dataflow_dttm
                           , '{{ workflow_id }}' as dataflow_id
                    from {{ tmp_relation_load_final }}
                    where flg_to_scd1_tmp in (1,2);

            {% endif %}

        {% endif %}

    {%- endset -%}

    {%- do return(dml) -%}

{%- endmacro -%}
