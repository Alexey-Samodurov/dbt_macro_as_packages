{% macro _validate_granularity_dim(granularity_dim) %}
  {% if granularity_dim not in ('days','months','years') -%}
    {% do adapter.raise_compiler_error('ERROR: invalid partition_granularity: ' ~ adapter.quote(granularity_dim) ~ ' .') %}
  {%- endif -%}
{% endmacro %}

{% macro _validate_combination_partition_column_current_date_prolongation(source_relation_fields, partition_column, current_date_prolongation) %}
  {% if not current_date_prolongation and partition_column not in source_relation_fields| map(attribute='name') | list -%}
    {% do adapter.raise_compiler_error("ERROR: current_date_prolongation is False but " ~ partition_column ~" doesn't exists in source table.") %}
  {%- endif -%}
{% endmacro %}


{% macro greenplum__check_table_has_default_partition(target_relation) %}
  {% call statement('get_check_partitioned_query', fetch_result=True, auto_begin=False) %}
      SELECT COUNT(*) as cnt 
      FROM pg_catalog.pg_partitions 
      WHERE schemaname = '{{target_relation.schema}}'
        AND  tablename = '{{target_relation.name}}' 
        AND partitiontype = 'range' 
        and partitionisdefault=True
  {% endcall %}
  {% do return(load_result('get_check_partitioned_query').table.columns.cnt|first|int) %}
{% endmacro %}  

{% macro greenplum__split_partition_between_dates(target_relation, min_value, max_value, granularity_digit, granularity_dim) %}
    {% call statement('get_dates_diff', fetch_result=True, auto_begin=False) %}
        select ceil((case when '{{granularity_dim}}' = 'days' then dates_diff
                    when '{{granularity_dim}}' = 'months' then dates_diff/28
                    when '{{granularity_dim}}' = 'years' then dates_diff/365
                    end ) / {{granularity_digit}}) as dates_diff
      from (SELECT ('{{max_value}}'::date - '{{min_value}}'::date)::numeric as dates_diff) t
    {% endcall %}
    {% set dates_diff = load_result('get_dates_diff').table.columns.dates_diff|first|int %}
    {% for i in range(dates_diff+1) %}
        {% set gran_value = i * granularity_digit|int %}      
        {#-- 1. Get start date for partition of current iteration date)   #}
        {% call statement('get_partition_date', fetch_result=True, auto_begin=False) %}
            select ('{{min_value}}'::date + make_interval({{granularity_dim}}:= {{i}} * {{granularity_digit}} ) )::date as current_partition_date,
                   ('{{min_value}}'::date + make_interval({{granularity_dim}}:= ({{i}} * {{granularity_digit}}) + {{granularity_digit}} ) )::date as next_partition_date
        {% endcall %}
        {% set current_partition_date = load_result('get_partition_date').table.columns.current_partition_date|first %}
        {% set current_next_partition_date = load_result('get_partition_date').table.columns.next_partition_date|first %}

        {#-- 2. Define current partition_name )   #}
        {% set partition_name = 'p_' ~ target_relation.name ~ '_' ~ (current_partition_date|replace('-','')) %}
        {{ log("Current partition name:"~partition_name)  }}
      
        {#-- 3. If partition not exists define and run split default partition )   #}
        {% call statement('get_check_partition_exists_query', fetch_result=True, auto_begin=False) %}
            SELECT COUNT(*) as cnt 
            FROM pg_catalog.pg_partitions 
            WHERE schemaname= '{{target_relation.schema}}' 
            AND tablename = '{{target_relation.name}}' 
            AND SUBSTRING(partitionrangeend FROM '''([^'']+)')::TIMESTAMP > '{{ current_partition_date }}'::timestamp
	          AND SUBSTRING(partitionrangestart FROM '''([^'']+)')::TIMESTAMP < '{{ current_next_partition_date }}'::timestamp
        {% endcall %}
        {% set check_partition_exists = load_result('get_check_partition_exists_query').table.columns.cnt|first|int %}
        
        {% if check_partition_exists == 0 %}
            {#-- 4. If partition not exists define and run split default partition )   #}
            {{ log('Partition '~partition_name~' not exists yet')  }}
            {% set split_partition_query %}
                ALTER TABLE {{target_relation.schema}}.{{target_relation.name}} 
                SPLIT DEFAULT PARTITION start ('{{current_partition_date}}'::date) 
                    INCLUSIVE end ('{{current_next_partition_date}}'::date) 
                    EXCLUSIVE into (partition {{partition_name}}, default partition)
            {% endset %}
            {{ log("Creating partition "~partition_name~" from default:"~split_partition_query)  }}
            {% do run_query(split_partition_query) %}
        {% else %}
            {{ log("Partition "~partition_name~" already exists")  }}
        {% endif %}  
    {% endfor %}
{% endmacro %} 

{% macro greenplum__manage_to_add_partition(target_relation, min_partition_value, max_partition_value, partition_column, partition_granularity) %}
  {% if partition_granularity| wordcount() == 2 -%}
    {%- set partition_granularity_list = partition_granularity.split(' ') | list -%}
    {%- set granularity_digit = partition_granularity_list[0] -%} 
    {%- set granularity_dim = partition_granularity_list[1]  -%} 
  {% else %}  
    {%- set granularity_digit = 1 -%} 
    {%- set granularity_dim = partition_granularity -%} 
  {%- endif -%}

  {%- set granularity_dim = granularity_dim ~ 's' if granularity_dim != None and granularity_dim != '' and granularity_dim[-1] != 's' else granularity_dim -%} 

  {% do _validate_granularity_dim(granularity_dim) %}
  {% if min_partition_value != none -%}
    {% call statement('get_check_partitioned_query', fetch_result=True, auto_begin=False) %}
      SELECT (min_existed::date - make_interval({{granularity_dim}}:= 
       		  ceil((case when '{{granularity_dim}}' = 'days' then (min_existed - '{{min_partition_value}}')
                      when '{{granularity_dim}}' = 'months' then (min_existed - '{{min_partition_value}}')::numeric/28
                      when '{{granularity_dim}}' = 'years' then (min_existed - '{{min_partition_value}}')::numeric/365
                      end ) / {{granularity_digit}})::int * {{granularity_digit}}) )::date as min_expected,																						   
         '{{max_partition_value}}'::date >= max_existed as max_flg,
         '{{min_partition_value}}'::date < min_existed as min_flg,
         max_existed,
         min_existed  
      FROM  
        (SELECT 
          MAX(SUBSTRING(partitionrangeend FROM '''([^'']+)'))::Date as max_existed,
          MIN(SUBSTRING(partitionrangestart FROM '''([^'']+)'))::Date as min_existed
        FROM pg_catalog.pg_partitions 
        WHERE schemaname = '{{target_relation.schema}}'
          AND  tablename = '{{target_relation.name}}') t 
    {% endcall %}
    {% set partitioned_info = load_result('get_check_partitioned_query').table %} 
    {% if partitioned_info.columns.max_flg|first  == none -%}
      {% if granularity_dim == 'months' %}
          {% set min_partition_value = min_partition_value.replace(day=1) %}
      {% elif granularity_dim == 'years' %}
          {% set min_partition_value = min_partition_value.replace(day=1).replace(month=1) %}
      {% endif %}
      {% do greenplum__split_partition_between_dates(target_relation, min_partition_value, max_partition_value, granularity_digit, granularity_dim) %}  
    {%- else -%}
        {% if partitioned_info.columns.max_flg|first  == 1 -%}
              {% do greenplum__split_partition_between_dates(target_relation, partitioned_info.columns.max_existed|first, max_partition_value, granularity_digit, granularity_dim) %} 
        {%- endif -%}
        {% if partitioned_info.columns.min_flg|first  == 1 -%}
              {% do greenplum__split_partition_between_dates(target_relation, partitioned_info.columns.min_expected|first, partitioned_info.columns.min_existed|first, granularity_digit, granularity_dim) %} 
        {%- endif -%}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}

{% macro greenplum__prolong_range_partition(target_relation, source_relation, partition_column, partition_granularity, current_date_prolongation = False) -%}
    {%- set source_relation_fields = greenplum__get_columns_in_relation(source_relation) -%} 
    {% do _validate_combination_partition_column_current_date_prolongation(source_relation_fields, partition_column, current_date_prolongation) %}
    {% if not current_date_prolongation or current_date_prolongation | string | lower == 'false' -%}
      {%- set max_min_partition_value = greenplum__get_min_max_part_column_query(partition_column, source_relation) -%}
    {%- else -%}
      {%- set max_min_partition_value ={ 'max' : modules.datetime.datetime.utcnow().date(), 'min' : modules.datetime.datetime.utcnow().date()} -%}
    {%- endif -%}
    {% if greenplum__check_table_has_default_partition(target_relation) -%}
      {% do greenplum__manage_to_add_partition(target_relation, max_min_partition_value['min'], max_min_partition_value['max'], partition_column, partition_granularity ) -%}
    {%- else -%}
      {% do adapter.raise_compiler_error("ERROR: table not partitioned by range or default partition doesn't exist: " ~ target_relation ~".") %}
    {%- endif -%}
    {{ return(max_min_partition_value) }}
{% endmacro %}

