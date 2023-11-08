{% macro _get_cut_param_table(table_name = 'grp_meta.cut_param', search_relation = None) %}
    {# form cut_param query #}
    {%- call statement('get_cut_param_data', fetch_result=True,auto_begin=false) -%}
        select 
            table_nm,
            column_nm,
            cut_value
        from {{ table_name }} c
        {% if search_relation != None %}
            where upper( c.job_nm ) = upper( '{{ this }}' )
                and upper( c.table_nm ) = upper( '{{ search_relation }}' )
        {% endif %}
    {%- endcall -%}
    {{ return(load_result('get_cut_param_data').table) }}
{% endmacro %}

{% macro _get_default_value_by_dtype(type_cut_field) %}
    {% set res %}
        {%- if type_cut_field in ("TIMESTAMP","DATE", "DATE_TO_TEXT", "TIMESTAMP WITH TIME ZONE") -%}
            1970-01-01
        {%- elif type_cut_field in ("BIGINT","INT","SERIAL","BIGSERIAL","INTEGER")  -%}
            0
        {%- else -%}
            {% do adapter.raise_compiler_error("ERROR: Unknown cut type: " ~ type_cut_field ~ ".") %}        
        {%- endif -%}
    {% endset %}
    {{ return(res) }}
{% endmacro %}

{% macro _get_default_cut_values_string() %}

    {# type field checks had been done before in greenplum__ods_validate_configs macro #}
    {%- set type_cut_field = config.get('type_cut_field', default=None).split(',')|map("trim")|map("upper")|join(',')|string -%}

    {% set res %}
        {%- for type_cut_field in type_cut_field.split(',') -%}
            {{ _get_default_value_by_dtype(type_cut_field) }}
            {%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
    {% endset %}

    {{ return(res) }}
{% endmacro %}

{% macro _validate_cut_params(param_names, cut_values, type_cut_field) %}
    {% if param_names|length != cut_values|length %}
        {% do adapter.raise_compiler_error('CUT PARAMS ERROR: Number of input cut param names in set_cut_param macro is not the same with cut_values in cut_param table.') %}
    {% endif %}
    {% if type_cut_field|length != cut_values|length %}
        {% do adapter.raise_compiler_error('CUT PARAMS ERROR: Number of type cut fields with number of cut_values in cut_param table.') %}
    {% endif %}
{% endmacro %}

{% macro set_cut_param(relation, param_name='dataflow_dttm', table_nm_relation=None) -%}
    {%- set param_names = param_name.split(',') -%}
    {% set get_cut_param_value_from = relation %}
    {% set get_table_nm_from = table_nm_relation if table_nm_relation != None else relation %}

    {# get data from cut_param table or get default values if table does not exists or cut_value is null #}
    {% set cut_param_meta = get_cut_param(get_table_nm_from, return_full_settings_map = True) %}
    
    {%- if execute -%}
        {% set meta_has_relation_data = cut_param_meta.record_exists %}

        {%- set cut_values = cut_param_meta.cut_values.split(',') -%}
        {% set type_cut_field = cut_param_meta.type_cut_field.split(',') %}
        {% do _validate_cut_params(param_names, cut_values, type_cut_field) %}

        {% if meta_has_relation_data %}

            {%- set select_param_name_str -%}
                {%- for p_name_idx in range(0,param_names|length) -%}
                    coalesce(
                        max( {{ param_names[p_name_idx] }} )::text,
                        '{{ cut_values[p_name_idx] }}'
                    )
                    {%- if not loop.last -%} {{ SPACE }}||','||{{ SPACE }} {%- endif -%}
                {%- endfor -%}
            {%- endset -%}   

            {%- set where_param_str -%}
                {%- for col in range(0,param_names|length) -%}
                
                    {%- if type_cut_field[col] in ("TIMESTAMP", "DATE", "TIMESTAMP WITH TIME ZONE") -%}
                        {{ param_names[col] }}::timestamp > '{{ cut_values[col] }}'::timestamp

                    {%- elif type_cut_field[col] in ("BIGINT", "INT","SERIAL","BIGSERIAL","INTEGER") -%}
                        {{ param_names[col]}}> '{{ cut_values[col] }}'::bigint

                    {# Kind of forbidden magic. Should not be used unless DDL-tool data integration #}
                    {%- elif type_cut_field[col] in ("DATE_TO_TEXT") -%}
                        {{ param_names[col]}}> '{{ cut_values[col] }}'::date::text
                    {%- endif -%}

                    {%- if not loop.last -%}{{ SPACE }} or {{ SPACE }}{%- endif -%}
                {%- endfor -%}
            {%- endset -%}
            {%- call statement('update_cut_param', fetch_result=false,auto_begin=false) -%}

            UPDATE grp_meta.cut_param c
            SET dataflow_dttm = date_trunc('sec', current_timestamp) at time zone '0',
                cut_value = (select {{ select_param_name_str }} from {{ get_cut_param_value_from }} where {{ where_param_str }} )
            where upper(c.job_nm) = upper( '{{ this }}' )
                and upper(c.table_nm) = upper( '{{ get_table_nm_from }}' )

            {%- endcall -%}

        {% else %}

            {%- set select_param_name_str -%}
                {%- for p_name_idx in range(0,param_names|length) -%}
                    coalesce(
                        max( {{ param_names[p_name_idx] }} )::text,
                        '{{_get_default_value_by_dtype(type_cut_field[p_name_idx])}}'
                    )
                    {%- if not loop.last -%} {{ SPACE }}||','||{{ SPACE }} {%- endif -%}
                {%- endfor -%}
            {%- endset -%}

            {%- call statement('update_cut_param', fetch_result=false,auto_begin=false) -%}

            INSERT INTO grp_meta.cut_param(dataflow_dttm, job_nm, table_nm, column_nm, cut_value)
            values(date_trunc('sec', current_timestamp) at time zone '0',
                upper( '{{ this }}' ),
                upper( '{{ get_table_nm_from }}' ),
                '{{ param_name }}',
                (select {{ select_param_name_str }} from {{ get_cut_param_value_from }} ))

            {%- endcall -%}
        {% endif %}

    {% else %}
        {% set meta_has_relation_data = False %}  
    {%- endif -%}

{%- endmacro %}

{% macro set_cut_params(relation_list, param_name='dataflow_dttm') -%}
    {% for relation in relation_list -%}
        {{ set_cut_param(relation, param_name) -}}
    {% endfor %}
{%- endmacro %}

{% macro get_cut_param(relation, return_full_settings_map = False) -%}

    {%- if execute -%}

        {# get cut_param table and extract data #}
        {% set cut_param_table = _get_cut_param_table(search_relation = relation) %}
        {% set table_record = cut_param_table.columns['table_nm'].values() | first %}
        {% set cut_value = cut_param_table.columns['cut_value'].values() | first %}

        {# parse results and place it to the result dictionary#}
        {% set result_dictionary = {'record_exists':False, 'cut_values':''} %}

        {# if record for the table exists insert True flag #}
        {# if record for cut_value exists insert values from table else insert default values #}
        {% if table_record and cut_value %}
            {% if result_dictionary.update({'record_exists':True}) %} {% endif %}
            {% if result_dictionary.update({'cut_values':cut_value}) %} {% endif %}
        {% elif table_record and not cut_value %}
            {% if result_dictionary.update({'record_exists':True}) %} {% endif %}
            {% if result_dictionary.update({'cut_values':_get_default_cut_values_string()}) %} {% endif %}
        {% else %}
            {% if result_dictionary.update({'record_exists':False}) %} {% endif %}
            {% if result_dictionary.update({'cut_values':_get_default_cut_values_string()}) %} {% endif %}
        {% endif %}
        {% if result_dictionary.update({'type_cut_field':config.get('type_cut_field', default=None).split(',')|map("trim")|map("upper")|join(',')|string}) %}{% endif %}

        {# macro can return either cut_values or settings_map (which is extandable) #}
        {% if not return_full_settings_map %}
            {{ return(result_dictionary.cut_values) }}
        {% else %}
            {{ return(result_dictionary) }}
        {% endif %}

    {%- endif -%}
{%- endmacro %}

{% macro get_cut_param_filter(relation, param_name='dataflow_dttm') -%}
    {%- if execute -%}
        {%- set type_cut_field = config.get('type_cut_field', default=None).split(',')|map("trim")|map("upper")|join(',')|string -%}
        {% if  type_cut_field in ("TIMESTAMP","DATE") -%}
            {{ param_name }} > ( select '{{ get_cut_param(relation) }}'::timestamp without time zone )
        {% elif  type_cut_field in ("TIMESTAMP WITH TIME ZONE") -%}
            {{ param_name }} > ( select '{{ get_cut_param(relation) }}'::timestamp with time zone )
        {%- elif type_cut_field in ("BIGINT","INT","SERIAL","BIGSERIAL","INTEGER")  -%}
            {{ param_name }} > ( select '{{ get_cut_param(relation) }}'::BIGINT )
        {%- elif type_cut_field in ("DATE_TO_TEXT")  -%}
            {{ param_name }} > ( select '{{ get_cut_param(relation) }}'::DATE::TEXT )
        {%- else  -%}
            {% do adapter.raise_compiler_error("ERROR: Unknown cut type: " ~ type_cut_field ~ ".") %}
        {%- endif %}
    {%- endif -%}
{%- endmacro %}
