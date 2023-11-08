{% macro greenplum__ods_temp_table_insert(
    relations_map,
    model_config,
    source_config,
    constants_map
)%}

    {# Prepare additional vars #}
    {% set min_max_data_from_snp = model_config['min_max_data_from_snp'] %}

    {# Generate code for hash column #}
    {% set hashdiff_expression = srv_get_hashdiff_str(relations_map.tmp.hash_attributes) %}

    {# Log warning in case if partition params are incorrect #}
    {% do _ods_validate_partitioning_params(model_config['partition_flg'], min_max_data_from_snp['min']) %}

    {% if not model_config['partition_flg'] or (model_config['partition_flg'] and min_max_data_from_snp['min'] == None) %}
        {% set ods_create_tmp_table_query = _ods_get_no_partition_tmp_load(
                                                src_relation        = relations_map.src.relation, 
                                                tmp_relation        = relations_map.tmp.relation, 
                                                colnames_as_string  = relations_map.tmp.business_attrs ~ relations_map.tmp.tech_columns_src_to_tmp, 
                                                hashdiff_expression = hashdiff_expression, 
                                                where_str           = model_config['where_str_for_src'], 
                                                distribution_key    = model_config['distributed_by']) %}    
    {% else %}
        {% set interval_for_step = (min_max_data_from_snp['max'] - min_max_data_from_snp['min']) // model_config['partition_count'] %}
        {% set quote_for_date = greenplum__generate_quote(min_max_data_from_snp['pg_typeof'])  %}

        {% set ods_create_tmp_table_query = _ods_get_partitioned_tmp_load(
                                                        src_relation            = relations_map.src.relation, 
                                                        tmp_relation            = relations_map.tmp.relation, 
                                                        colnames_as_string      = relations_map.tmp.business_attrs ~ relations_map.tmp.tech_columns_src_to_tmp, 
                                                        hashdiff_expression     = hashdiff_expression, 
                                                        distribution_key        = model_config['distributed_by'],
                                                        partition_column_nm     = model_config['partition_column_nm'],
                                                        min_max_data_from_snp   = min_max_data_from_snp,
                                                        partition_count         = model_config['partition_count'] ) %}
    {% endif %}

    {% do run_query(ods_create_tmp_table_query) %}

    {% do relations_map.get('tmp').update({
        'columns' : greenplum__get_columns_in_relation(relations_map.tmp.relation)
    }) %}
    
    {% if constants_map.macro_filter_input_data != 'skip' %}
        {% set runnable_macro = context.get(constants_map.macro_filter_input_data) %}
        {% do runnable_macro(
            relations_map,
            model_config,
            constants_map
        ) %}
    {% endif %}
{% endmacro %}

{# ============= Temp load options ============= #}

{% macro _ods_get_no_partition_tmp_load(src_relation, tmp_relation, colnames_as_string, hashdiff_expression, where_str, distribution_key) %}
    {% set where_str = 'where '~ where_str if where_str != '' else where_str %}
    {% set ods_create_tmp_table_query %}
        create temp table {{ tmp_relation }} on commit drop as 
            select 
                {{ colnames_as_string }}, 
                {{ hashdiff_expression }}::text as hashdiff_key
            from {{ src_relation }}
            {{ where_str }}
            {{ distributed_by(distribution_key) }} 
    {% endset %}

    {{ return(ods_create_tmp_table_query)  }}
{% endmacro %}

{% macro _ods_get_partitioned_tmp_load(
    src_relation, 
    tmp_relation, 
    colnames_as_string, 
    hashdiff_expression, 
    distribution_key,
    partition_column_nm,
    min_max_data_from_snp,
    partition_count ) %}

    {% set interval_for_step = (min_max_data_from_snp['max'] - min_max_data_from_snp['min']) // partition_count %}
    {% set quote_for_date = greenplum__generate_quote(min_max_data_from_snp['pg_typeof'])  %}

    {% set ods_create_tmp_table_query %}
    {%- for step in range(1,partition_count+1) -%}
        {% if loop.first %} 
            create temp table {{ tmp_relation }} on commit drop as 
        {% else %} 
            insert into {{ tmp_relation }} 
        {% endif %} 
            select 
                {{ colnames_as_string }},
                {{ hashdiff_expression }}::text as hashdiff_key
            from {{ src_relation }} 
            where {% if loop.first %}  
                    {{ partition_column_nm }} is null or 
                {% else %} 
                    {{ partition_column_nm }} >= {{ quote_for_date }}{{ (min_max_data_from_snp['min'] + interval_for_step*(step-1)) }}{{ quote_for_date }}   {% if not loop.last %} and {% endif %}  
                {% endif %} 
                {% if not loop.last %}  
                    {{ partition_column_nm }}  <  {{ quote_for_date }}{{ (min_max_data_from_snp['min'] + interval_for_step*step) }}{{ quote_for_date }} 
                {% endif %} 
            {% if loop.first %} {{ distributed_by(distribution_key) }} {% endif %} ;
        {%- endfor -%}
    {%- endset -%}

    {{ return(ods_create_tmp_table_query)  }}
{% endmacro %}


{# ============= Deduplication steps ============= #}
{# ============= helping macro ============= #}

{#/* fix cdc_type_code inside single load */ #}
{% macro _ods_get_cdc_code_fix_inside_partition_es(cdc_type_field_name, prev_cdc_type_field_name) %}
    {%- set fix_cdc_type_code -%}
        case 
            when {{prev_cdc_type_field_name}} = 'I' and {{cdc_type_field_name}} = 'I' then 'U'
            when {{prev_cdc_type_field_name}} = 'D' and {{cdc_type_field_name}} = 'I' then 'U'
            when {{prev_cdc_type_field_name}} = 'U' and {{cdc_type_field_name}} = 'I' then 'U'
            else {{cdc_type_field_name}}
        end
    {%- endset -%}
    {{ return(fix_cdc_type_code) }}
{% endmacro %}

{# /* fix cdc_type_code between loads */#}
{% macro _ods_get_cdc_code_fix_between_partitions_es(cdc_type_code_field_name, load_key, previous_cdc_type_field) %}
    {%- set fix_cdc_type_code -%}

        case 
            when trg.{{ load_key.split(",",1)[0] }} is null
                and incr.{{ previous_cdc_type_field }} is null
                and incr.{{cdc_type_code_field_name}} = 'U'
            then 'I'

            when trg.{{ load_key.split(",",1)[0] }} is not null
                and incr.{{ previous_cdc_type_field }} is null
                and incr.{{cdc_type_code_field_name}} = 'I'
            then 'U'

            else incr.{{cdc_type_code_field_name}}
        end

    {%- endset -%}
    {{ return(fix_cdc_type_code) }}
{% endmacro %}

{% macro _ods_create_prefinal_table(
                    output_relation, 
                    output_columns, 
                    incr_relation, 
                    snp_relation, 
                    join_clause, 
                    target_hashdiff_relation, 
                    distribution_key,
                    load_key, 
                    rn_asc_name, 
                    rn_desc_name, 
                    cdc_type_fieldname,
                    previous_cdc_type_field,
                    version_field,
                    target_partition_prunning_str ) %}

    {% set prefinal_table_query %}
        create temp table {{ output_relation }} on commit drop as 
            select 
                incr.{{ output_columns|join(', incr.')|string }},
                incr.{{rn_desc_name}},
                {{ _ods_get_cdc_code_fix_between_partitions_es(cdc_type_fieldname, load_key, previous_cdc_type_field) }} as cdc_type_code,
                case 
                    when {{ _ods_get_cdc_code_fix_between_partitions_es(cdc_type_fieldname, load_key, previous_cdc_type_field) }} = 'D'
                        then 1
                    else 0 end as snp_delete_flg
            from {{ incr_relation }} incr
            left join {{ snp_relation }} trg
                on {{ join_clause|join(" and ") }}
                {% if target_partition_prunning_str != None %} and trg.{{target_partition_prunning_str}} {% endif %}
            where incr.{{version_field}} > trg.{{version_field}} or trg.{{version_field}} is null
                and case
                        when incr.{{cdc_type_fieldname}} = 'D' then True
                        when incr.{{cdc_type_fieldname}} in ('I', 'U') and incr.{{rn_asc_name}} != 1 then True
                        when incr.{{cdc_type_fieldname}} in ('I', 'U') and incr.{{rn_asc_name}} = 1 and incr.hashdiff_key != {{target_hashdiff_relation}} then True
                        else False
                    end
            {{ distributed_by(distribution_key) }} 
    {% endset %}

    {% do run_query(prefinal_table_query) %}
{% endmacro %}

{# ============= event-streaming start =============#}
{% macro _ods_deduplicate_by_row_hashdiff(
                    output_relation, 
                    relation_to_deduplicate, 
                    output_columns, 
                    partition_by, 
                    rn_asc_name, 
                    rn_desc_name, 
                    cdc_type_fieldname,
                    previous_cdc_type_field,
                    previous_hash_field
                    ) %}

    {% set fill_deduplication_view_query %}
        create view {{ output_relation }} as 
        select {{output_columns|join(', ')|string}},
            {{previous_cdc_type_field}},
            {{_ods_get_cdc_code_fix_inside_partition_es(cdc_type_fieldname, previous_cdc_type_field)}} as {{ cdc_type_fieldname }},
            row_number() over (partition by {{ partition_by }} order by ce_version) as {{rn_asc_name}},
            row_number() over (partition by {{ partition_by }} order by ce_version desc) as {{rn_desc_name}}
        from (
            select {{output_columns|join(', ')|string }},
                {{cdc_type_fieldname}},
                lag(hashdiff_key) over (partition by {{ partition_by }} order by ce_version) as {{previous_hash_field}},
                lag({{cdc_type_fieldname}}) over (partition by {{ partition_by }} order by ce_version) as {{previous_cdc_type_field}}
            from {{ relation_to_deduplicate }} 
        ) deduplicate 
        where not (
            {{previous_hash_field}} is not null
            and hashdiff_key = {{previous_hash_field}}
            and coalesce({{cdc_type_fieldname}} || {{previous_cdc_type_field}}, 'DI') not in ('DI','DU','ID','UD')
        )
    {% endset %}
    {% do run_query(fill_deduplication_view_query) %}
{% endmacro %}


{% macro greenplum__ods_filter_input_data_type_es(relations_map, model_config, constants_map) %}

    {% set PREVIOUS_CDC_TYPE_FIELD = 'previous_cdc_type' %}
    {% set PREVIOUS_HASH_FIELD = 'previous_hash' %}
    {% set VERSION_FIELDNAME = 'ce_version' %}

    {# RM cdc_type_field here to override it in later steps #}
    {% set tmp_relation_columns = relations_map.tmp.columns 
                                            | map(attribute='name') 
                                            | reject('in', model_config['cdc_type_field'].split(","))
                                            | list %}
    

    {% set deduplication_v01 = greenplum__ods_make_relation( 
                                    base_relation = relations_map.tmp.relation, 
                                    suffix = 'deduplication_v') %}
    {% do _ods_deduplicate_by_row_hashdiff(
                            output_relation         =   deduplication_v01,
                            output_columns          =   tmp_relation_columns,
                            relation_to_deduplicate =   relations_map.tmp.relation,
                            partition_by            =   model_config['load_key'],
                            rn_asc_name             =   constants_map['ASC_ROW_NUMERATION_FIELD'],
                            rn_desc_name            =   constants_map['DESC_ROW_NUMERATION_FIELD'],
                            cdc_type_fieldname      =   model_config['cdc_type_field'],
                            previous_cdc_type_field =   PREVIOUS_CDC_TYPE_FIELD,
                            previous_hash_field     =   PREVIOUS_HASH_FIELD
                            ) %}

    {% set prefinal_relation = greenplum__make_temp_relation(relations_map.base.relation, '_prefinal') %}
    {% set snp_hashdiff_expression = srv_get_hashdiff_str(relations_map.snp.hash_attributes, 'trg') %}
    {% set join_clause = greenplum__ods_generate_join_conditions(model_config['load_key'], 'incr.', 'trg.') %}

    {% do _ods_create_prefinal_table(
        output_relation                 = prefinal_relation,
        output_columns                  = tmp_relation_columns,
        incr_relation                   = deduplication_v01, 
        snp_relation                    = relations_map.snp.relation,
        join_clause                     = join_clause, 
        target_hashdiff_relation        = snp_hashdiff_expression, 
        distribution_key                = model_config['distributed_by'],
        load_key                        = model_config['load_key'],
        rn_asc_name                     = constants_map['ASC_ROW_NUMERATION_FIELD'],
        rn_desc_name                    = constants_map['DESC_ROW_NUMERATION_FIELD'],
        cdc_type_fieldname              = model_config['cdc_type_field'],
        previous_cdc_type_field         = PREVIOUS_CDC_TYPE_FIELD, 
        version_field                   = VERSION_FIELDNAME,         
        target_partition_prunning_str   = model_config['target_partition_prunning_str']
    ) %}

    {% do relations_map.get('tmp').update({
        'relation' : prefinal_relation
    })%}

{% endmacro %}

{# ============= event-streaming end =============#}
{# ============= message-lib start =============#}

{% macro _ods_get_cdc_code_inside_paritition_msg_lib(snapshot_datetime_field, prev_hash_name) %}
    {%- set fix_cdc_type_code -%}
        case 
            when {{ snapshot_datetime_field }} ilike '#%' 
                then 'D'
            when {{ prev_hash_name }} is null  
                then 'I'
                else 'U' 
        end
    {%- endset -%}
    {{ return(fix_cdc_type_code) }}
{% endmacro %}

{% macro _ods_get_cdc_code_between_paritition_msg_lib(load_key, cdc_type_code_name) %}
    {%- set fix_cdc_type_code -%}
        case 
            when trg.{{ load_key.split(",",1)[0] }} is not null 
                and {{cdc_type_code_name}} = 'I'
                then 'U'
                else {{cdc_type_code_name}}
            end
    {%- endset -%}
    {{ return(fix_cdc_type_code) }}
{% endmacro %}

{% macro _ods_deduplicate_snapshot_by_row_hashdiff_msg_lib(
                    output_relation, 
                    relation_to_deduplicate, 
                    output_columns, 
                    partition_by,
                    distribution_key, 
                    rn_asc_name, 
                    rn_desc_name, 
                    cdc_type_fieldname,
                    previous_hash_field,
					snapshot_datetime_field,
                    snapshot_time_field_type
                    ) %}

    {% set create_deduplicated_table_query %}
        create temp table {{ output_relation }} on commit drop as 
            with snap_date_in_incr as (
                select {{ snapshot_datetime_field }} as snap_date
                        , lead({{ snapshot_datetime_field }},1,'5999-01-01') over (order by {{ snapshot_datetime_field }}) as lead_snap_date
                from {{ relation_to_deduplicate }}
                group by {{ snapshot_datetime_field }}),
            incr_distinct_on_pk_in_snap as (
                select distinct on ({{ snapshot_datetime_field }},{{ partition_by }}) {{output_columns|join(', ')|string}}
                       , {{ snapshot_datetime_field }}
                from {{ relation_to_deduplicate }}),
            calc_hash_snp_del as (
		         select {{output_columns|join(', ')|string}}
                       ,'U' as cdc_type_code 
		                ,{{ snapshot_datetime_field }} as snapshot_datetime_for_order
                       ,case when lead({{ snapshot_datetime_field }},1,'5999-01-01') over (partition by {{ partition_by }} order by {{ snapshot_datetime_field }}) != lead_snap_date 
                            then {{ snapshot_datetime_field }}::text || '#$#'||lead_snap_date::text
                            else {{ snapshot_datetime_field }}::text end {{ snapshot_datetime_field }}
                from incr_distinct_on_pk_in_snap src
                join snap_date_in_incr 
                on snap_date_in_incr.snap_date = src.{{ snapshot_datetime_field }}),
            unnest_del_row as (
		        select {{output_columns|join(', ')|string}}
                        , unnest(string_to_array({{ snapshot_datetime_field }}, '$')) as {{ snapshot_datetime_field }}
                        , lag(hashdiff_key) over (partition by {{ partition_by }} order by snapshot_datetime_for_order) as {{previous_hash_field}}
               from calc_hash_snp_del)
		    select {{output_columns|join(', ')|string}}
                   , _ods_get_cdc_code_inside_paritition_msg_lib(snapshot_datetime_field, {{previous_hash_field}}) as {{cdc_type_fieldname}}
                   , case 
                        when _ods_get_cdc_code_inside_paritition_msg_lib(snapshot_datetime_field, {{previous_hash_field}}) = 'D'
                            then 1
                            else 0
                        end as snp_delete_flg
                   , trim({{ snapshot_datetime_field }},'#')::{{ snapshot_time_field_type }} as {{ snapshot_datetime_field }}
                   , row_number() over (partition by {{ partition_by }} order by trim({{ snapshot_datetime_field }},'#')::{{ snapshot_time_field_type }}) as {{rn_asc_name}}
                   , row_number() over (partition by {{ partition_by }} order by trim({{ snapshot_datetime_field }},'#')::{{ snapshot_time_field_type }} desc) as {{rn_desc_name}}
            from unnest_del_row src
            where {{previous_hash_field}} is null or hashdiff_key <> {{previous_hash_field}} or {{ snapshot_datetime_field }} ilike '#%'
            {{ distributed_by(distribution_key) }} 
    {% endset %}
    {% do run_query(create_deduplicated_table_query) %}
{% endmacro %}

{% macro _ods_create_prefinal_table_snapshot_msg_lib(
                    output_relation, 
                    business_attrs, 
                    incr_relation, 
                    snp_relation, 
                    join_clause, 
                    target_hashdiff_relation, 
                    distribution_key,
                    load_key, 
                    rn_asc_name, 
                    rn_desc_name,
                    snapshot_datetime_field,
                    max_snapshot_date_has_loaded,
                    min_snapshot_date_in_incr,
                    tech_columns,
                    cdc_type_fieldname) %} 


    {% set prefinal_table_query %}
        create temp table {{ output_relation }} on commit drop as 	
            select 
                incr.{{ business_attrs|join(', incr.')|string }}
                , incr.{{rn_desc_name}}
                , _ods_get_cdc_code_between_paritition_msg_lib(load_key, 'incr.{{cdc_type_fieldname}}') as {{cdc_type_fieldname}}
                , case 
                    when _ods_get_cdc_code_between_paritition_msg_lib(load_key, 'incr.{{cdc_type_fieldname}}') = 'D'
                        then 1
                        else 0
                    end as snp_delete_flg
                , incr.{{snapshot_datetime_field}}
                , incr.{{ tech_columns|join(', incr.')|string }}
            from {{ incr_relation }} incr
            left join {{ snp_relation }} trg
                on {{ join_clause|join(" and ") }}
            where incr.{{snapshot_datetime_field}} > '{{max_snapshot_date_has_loaded}}'
                and case
                        when incr.cdc_type_code = 'D' then True
                        when incr.cdc_type_code in ('I', 'U') and incr.{{rn_asc_name}} != 1 then True
                        when incr.cdc_type_code in ('I', 'U') and incr.{{rn_asc_name}} = 1 and incr.hashdiff_key != {{target_hashdiff_relation}} then True
                        else False
                    end
			union all
            select 
                trg.{{ business_attrs|join(', trg.')|string }}
                , 1 as {{rn_desc_name}}
                ,'D' as cdc_type_code
                , 1 as snp_delete_flg
                , '{{min_snapshot_date_in_incr}}' as {{snapshot_datetime_field}}
                , incr.{{ tech_columns|join(', incr.')|string }}
            from {{ snp_relation }} trg 
            left join {{ incr_relation }} incr
                on {{ join_clause|join(" and ") }}
			where incr.{{ load_key.split(',')[0] }} is null
            {{ distributed_by(distribution_key) }} 
    {% endset %}

    {% do run_query(prefinal_table_query) %}
{% endmacro %}

{% macro greenplum__ods_filter_input_data_type_msg_lib(relations_map, model_config, constants_map) %}

    {% set PREVIOUS_HASH_FIELD = 'previous_hash' %}
    {% set CDC_TYPE_FIELD_NAME = 'cdc_type_code' %}

    {# RM snapshot_time_field here to override it in later steps #}
    {% set tmp_relation_columns = relations_map.tmp.columns 
                                            | map(attribute='name')
                                            | reject ('in' , [model_config['snapshot_time_field']])
                                            | list %}
     {% set cut_tech_columns = tmp_relation_columns
                                            | reject ('in' , relations_map.tmp.business_attrs.split(','))
                                            | list %}
     {% set business_attrs = tmp_relation_columns| reject ('in' , cut_tech_columns) | list%}

    {% set deduplication_v01 = greenplum__ods_make_relation( 
                                    base_relation = relations_map.tmp.relation, 
                                    suffix = 'deduplication_v') %}
    {% do _ods_deduplicate_snapshot_by_row_hashdiff_msg_lib(
                            output_relation               =   deduplication_v01,
                            output_columns                =   tmp_relation_columns,
                            relation_to_deduplicate       =   relations_map.tmp.relation,
                            partition_by                  =   model_config['load_key'],
                            distribution_key                = model_config['distributed_by'],
                            rn_asc_name                   =   constants_map['ASC_ROW_NUMERATION_FIELD'],
                            rn_desc_name                  =   constants_map['DESC_ROW_NUMERATION_FIELD'],
                            cdc_type_fieldname            =   CDC_TYPE_FIELD_NAME,
                            previous_hash_field           =   PREVIOUS_HASH_FIELD,
                            snapshot_datetime_field       =   model_config['snapshot_time_field'],
                            snapshot_time_field_type      =   model_config['snapshot_time_field_type']
                            ) %}

    {% set prefinal_relation = greenplum__make_temp_relation(relations_map.base.relation, '_prefinal') %}
    {% set snp_hashdiff_expression = srv_get_hashdiff_str(relations_map.snp.hash_attributes, 'trg') %}
    {% set join_clause = greenplum__ods_generate_join_conditions(model_config['load_key'], 'incr.', 'trg.') %}
    
    {%- call statement('get_query_results', fetch_result=True,auto_begin=false) -%}
      select coalesce(max({{ model_config['snapshot_time_field']}}),'1970-01-01'::date) as max
      from {{ relations_map.snp.relation }}
      where {{ model_config['snapshot_time_field']}} is not null
    {%- endcall -%}
    {% set max_snapshot_date_has_loaded = load_result('get_query_results').table.columns.max|first %}

    {%- call statement('get_query_results', fetch_result=True,auto_begin=false) -%}
      select min({{ model_config['snapshot_time_field']}})
      from {{ relations_map.tmp.relation }}
      where {{ model_config['snapshot_time_field']}} is not null
    {%- endcall -%}
    {% set min_snapshot_date_in_incr = load_result('get_query_results').table.columns.min|first %}

    {% if min_snapshot_date_in_incr != None %}
      {% do _ods_create_prefinal_table_snapshot_msg_lib(
        output_relation                 = prefinal_relation,
        business_attrs                  = business_attrs,
        incr_relation                   = deduplication_v01, 
        snp_relation                    = relations_map.snp.relation,
        join_clause                     = join_clause, 
        target_hashdiff_relation        = snp_hashdiff_expression, 
        distribution_key                = model_config['distributed_by'],
        load_key                        = model_config['load_key'],
        rn_asc_name                     = constants_map['ASC_ROW_NUMERATION_FIELD'],
        rn_desc_name                    = constants_map['DESC_ROW_NUMERATION_FIELD'],
        snapshot_datetime_field         = model_config['snapshot_time_field'],
        max_snapshot_date_has_loaded    = max_snapshot_date_has_loaded,
        min_snapshot_date_in_incr       = min_snapshot_date_in_incr,
        tech_columns                    = cut_tech_columns,
        cdc_type_fieldname              = CDC_TYPE_FIELD_NAME
      ) %}

      {% do relations_map.get('tmp').update({
          'relation' : prefinal_relation
      })%}
    {%- else -%} 
       {% do relations_map.get('tmp').update({
          'relation' : deduplication_v01
      })%}    
    {%- endif-%}
{% endmacro %}
{# ============= message-lib end =============#}
{# ============= s3_incr start =============#}
{% macro _ods_deduplicate_by_row_hashdiff_s3_incr(
                    output_relation, 
                    relation_to_deduplicate, 
                    output_columns, 
                    partition_by, 
                    rn_asc_name, 
                    rn_desc_name, 
                    version_field,
                    previous_cdc_type_field,
                    previous_hash_field,
                    cdc_type_fieldname
                    ) %}

    {% set fill_deduplication_view_query %}
        create view {{ output_relation }} as 
        select {{output_columns|join(', ')|string}},
            case when {{rn_asc_name}} = 1 then NULL else 'I' end {{previous_cdc_type_field}},
            case when {{rn_asc_name}} = 1 then 'I' else 'U' end {{cdc_type_fieldname}},
            {{rn_asc_name}},
            row_number() over (partition by {{ partition_by }} order by {{version_field}} desc) as {{rn_desc_name}}
        from (
            select {{output_columns|join(', ')|string }},
                row_number() over (partition by {{ partition_by }} order by {{version_field}}) as {{rn_asc_name}},
                lag(hashdiff_key) over (partition by {{ partition_by }} order by {{version_field}}) as {{previous_hash_field}}
            from {{ relation_to_deduplicate }} 
        ) deduplicate 
        where  hashdiff_key is distinct from {{previous_hash_field}}
    {% endset %}
    {% do run_query(fill_deduplication_view_query) %}
{% endmacro %}

{% macro greenplum__ods_filter_input_data_type_s3_incr(relations_map, model_config, constants_map) %}

    {% set PREVIOUS_CDC_TYPE_FIELD = 'previous_cdc_type' %}
    {% set PREVIOUS_HASH_FIELD = 'previous_hash' %}
    {% set CDC_TYPE_FIELDNAME = 'cdc_type_code' %}    

    {# RM cdc_type_field here to override it in later steps #}
    {% set tmp_relation_columns = relations_map.tmp.columns 
                                            | map(attribute='name') 
                                            | list %}
    

    {% set deduplication_v01 = greenplum__ods_make_relation( 
                                    base_relation = relations_map.tmp.relation, 
                                    suffix = 'deduplication_v') %}
    {% do _ods_deduplicate_by_row_hashdiff_s3_incr(
                            output_relation         =   deduplication_v01,
                            output_columns          =   tmp_relation_columns,
                            relation_to_deduplicate =   relations_map.tmp.relation,
                            partition_by            =   model_config['load_key'],
                            rn_asc_name             =   constants_map['ASC_ROW_NUMERATION_FIELD'],
                            rn_desc_name            =   constants_map['DESC_ROW_NUMERATION_FIELD'],
                            version_field           =   model_config['version_field'],
                            previous_cdc_type_field =   PREVIOUS_CDC_TYPE_FIELD,
                            previous_hash_field     =   PREVIOUS_HASH_FIELD,
                            cdc_type_fieldname      =   CDC_TYPE_FIELDNAME
                            ) %}

    {% set prefinal_relation = greenplum__make_temp_relation(relations_map.base.relation, '_prefinal') %}
    {% set snp_hashdiff_expression = srv_get_hashdiff_str(relations_map.snp.hash_attributes, 'trg') %}
    {% set join_clause = greenplum__ods_generate_join_conditions(model_config['load_key'], 'incr.', 'trg.') %}

    {% do _ods_create_prefinal_table(
        output_relation                 = prefinal_relation,
        output_columns                  = tmp_relation_columns,
        incr_relation                   = deduplication_v01, 
        snp_relation                    = relations_map.snp.relation,
        join_clause                     = join_clause, 
        target_hashdiff_relation        = snp_hashdiff_expression, 
        distribution_key                = model_config['distributed_by'],
        load_key                        = model_config['load_key'],
        rn_asc_name                     = constants_map['ASC_ROW_NUMERATION_FIELD'],
        rn_desc_name                    = constants_map['DESC_ROW_NUMERATION_FIELD'],
        cdc_type_fieldname              = CDC_TYPE_FIELDNAME,
        previous_cdc_type_field         = PREVIOUS_CDC_TYPE_FIELD,
        version_field                   = model_config['version_field'],
        target_partition_prunning_str   = model_config['target_partition_prunning_str']
    ) %}

    {% do relations_map.get('tmp').update({
        'relation' : prefinal_relation
    })%}

{% endmacro %}

{# ============= s3_incr end =============#}
{# ============= jdbc-snap start =============#}

{% macro _ods_calculate_jdbc_snap_new_records(
    output_relation, 
    tmp_relation, 
    snp_relation,
    tmp_columns,
    load_key,
    target_partition_prunning,
    target_hashdiff_columns,
    cdc_type_field_name,
    snp_delete_flg_name,
    prev_delete_flg_name
) %}

    {% set tmp_join_clause = greenplum__ods_generate_join_conditions(load_key, 'tmp.', 'snp.') %}
    {% set snp_hashdiff_expression = srv_get_hashdiff_str(target_hashdiff_columns, 'snp') %}

    {% set jdbc_snap_calculate_cdc_type_code_query %}
    create temp table {{ output_relation }} on commit drop as 	
        with 
            new_cdc_rows(
                {{ tmp_columns|join(",") }},
                {{ cdc_type_field_name }},
                {{ snp_delete_flg_name }},
                {{ prev_delete_flg_name }}
            ) as (
                select 
                    tmp.{{ tmp_columns|join(',tmp.')|string }},
                    case
                        when snp.{{ load_key.split(',')[0] }} is null
                            then 'I'
                        when snp.{{ load_key.split(',')[0] }} is not null
                            and {{ snp_hashdiff_expression }} != tmp.hashdiff_key
                            then 'U'
                    end as {{ cdc_type_field_name }},
                    0 as {{ snp_delete_flg_name }},
                    0 as {{ prev_delete_flg_name }}
                from {{ tmp_relation }} tmp
                left join {{ snp_relation }} snp
                    on {{ tmp_join_clause|join(' and ') }}
                        and snp.{{snp_delete_flg_name}} = 0
                    {% if target_partition_prunning != None %} and snp.{{target_partition_prunning}} {% endif %}
            )
            select 
                {{ tmp_columns|join(",")|string }},
                {{ cdc_type_field_name }},
                {{ snp_delete_flg_name }},
                {{ prev_delete_flg_name }}
            from new_cdc_rows
    {% endset %}

    {% do run_query(jdbc_snap_calculate_cdc_type_code_query) %}
{% endmacro %}

{% macro _ods_jdbc_snap_add_delete_records(
    relation_to_insert,
    tmp_relation,
    snp_relation,
    tmp_columns,
    load_key,
    cdc_type_field_name,
    snp_delete_flg_name,
    prev_delete_flg_name
)%}

    {% set tmp_join_clause = greenplum__ods_generate_join_conditions(load_key, 'tmp.', 'snp.') %}

    {% set add_delete_records %}
        insert into {{relation_to_insert}}(
            {{ tmp_columns|reject('in', ['"cdc_source_dttm"','"cdc_target_dttm"', '"hashdiff_key"'])|join(",") }},
            {{ cdc_type_field_name }},
            {{ snp_delete_flg_name }},
            {{ prev_delete_flg_name }}, 
            cdc_source_dttm,
            cdc_target_dttm
        )
        select 
            snp.{{ tmp_columns|reject('in', ['"cdc_source_dttm"','"cdc_target_dttm"', '"hashdiff_key"'])|join(',snp.')|string }}, 
            'D' as {{cdc_type_field_name}}, 
            1   as {{snp_delete_flg_name}},
            case 
                when snp.snp_delete_flg = 1
                    then 1
                    else 0
            end as {{ prev_delete_flg_name }},
            now() as cdc_source_dttm,
            now() as cdc_target_dttm
        from {{ snp_relation }} snp
        left join {{ relation_to_insert }} tmp
            on {{ tmp_join_clause|join(' and ') }}
        where tmp.{{ load_key.split(',')[0] }} is null 
            or (snp.snp_delete_flg = 1 and tmp.{{cdc_type_field_name}} is null)

    {% endset %}
    {% do run_query(add_delete_records) %}
{% endmacro %}

{% macro _ods_jdbc_snap_get_cut_relation(relation, where_str, cut_field, compare_with_full_history) %}
    
    {% if not cut_field or (cut_field and compare_with_full_history) %}
        {% do return(relation) %}

    {% else %}
        {% set cut_relation = greenplum__make_temp_relation(relation, '_cut_relation') %}
        {% set create_cut_relation %}
            create temp table {{cut_relation}} on commit drop as 
            select * from {{ relation }} 
            where {{ where_str }} 
        {% endset %}
        {% do run_query(create_cut_relation) %}
        {{ return(cut_relation) }}
    {% endif %}

{% endmacro %}


{% macro greenplum__ods_filter_input_data_type_jdbc_snap(relations_map, model_config, constants_map) %}

    {% set CDC_TYPE_FIELDNAME = 'cdc_type_code' %}
    {% set SNP_DELETE_FLG_NAME = 'snp_delete_flg' %}
    {% set PREV_DELETE_FLG = 'prev_delete_flg' %}

    {% set tmp_relation_columns = relations_map.tmp.columns 
                                            | map(attribute='name') 
                                            | list %}
    {% set jdbc_cdc_status_prefinal_relation = greenplum__ods_make_relation( 
                                        base_relation = relations_map.tmp.relation, 
                                        suffix = 'jdbc_cdc_status') %}

    {% set snp_rows = _ods_jdbc_snap_get_cut_relation(
        relation = relations_map.snp.relation,
        where_str = model_config['where_str'],
        cut_field = model_config['cut_field'],
        compare_with_full_history = model_config['compare_with_full_history']
    ) %}
    {% set tmp_rows = _ods_jdbc_snap_get_cut_relation(
        relation = relations_map.tmp.relation,
        where_str = model_config['where_str'],
        cut_field = model_config['cut_field'],
        compare_with_full_history = model_config['compare_with_full_history']
    ) %}

    {% do _ods_calculate_jdbc_snap_new_records(
        output_relation = jdbc_cdc_status_prefinal_relation,
        tmp_relation = tmp_rows,
        snp_relation = snp_rows,
        tmp_columns = tmp_relation_columns,
        load_key = model_config['load_key'],
        target_partition_prunning = model_config['target_partition_prunning_str'],
        target_hashdiff_columns = relations_map.snp.hash_attributes,
        cdc_type_field_name = CDC_TYPE_FIELDNAME,
        snp_delete_flg_name = SNP_DELETE_FLG_NAME,
        prev_delete_flg_name = PREV_DELETE_FLG
    ) %}

    {# /* Identify cases when it is possible to calculate deleted records */ #}
    {% set delete_recs_calculatable_flg = True if not model_config['cut_field'] or (model_config['cut_field'] and not model_config['compare_with_full_history']) else False %}
    
    {# /* No need to calculate delete-records for Append load */ #}
    {% if model_config['load_type'] in ['R','U'] and delete_recs_calculatable_flg %}
        {% do _ods_jdbc_snap_add_delete_records(
            relation_to_insert = jdbc_cdc_status_prefinal_relation,
            tmp_relation = tmp_rows,
            snp_relation = snp_rows,
            tmp_columns = tmp_relation_columns,
            load_key = model_config['load_key'],
            cdc_type_field_name = CDC_TYPE_FIELDNAME,
            snp_delete_flg_name = SNP_DELETE_FLG_NAME,
            prev_delete_flg_name = PREV_DELETE_FLG
        ) %}
    {% endif %}
                                    
    {% do relations_map.get('tmp').update({
        'relation' : jdbc_cdc_status_prefinal_relation
    })%}

{% endmacro %}

{# ============= jdbc-snap end =============#}