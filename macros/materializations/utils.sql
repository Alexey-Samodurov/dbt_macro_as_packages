{%- macro srv_get_hashdiff_str(attributes, table_alias = none) -%}
    {%- set NON_ALIAS_VALUES = ['null', '"null"'] -%}
    {%- set delimeter = '#' -%}
    {%- set null_value_placeholder = '' -%}
    {%- set table_alias_str = '' if table_alias is none else table_alias ~ '.' -%}

    {# iterate over attributes and get md5 hash for their concatenation #}
    {# if attribute is null then replace by empty string #}
    {# regexp_replace removes null at the end of the string #}
    {# in order to equals the result for no column and empty column #}
    {%- set hashdiff -%}
		  md5(regexp_replace(
        rtrim(
                {%- for attr in attributes %}
                  trim(coalesce(
                    {%- if attr not in NON_ALIAS_VALUES -%} 
                      {{table_alias_str}} 
                    {%- endif -%}
                      {{attr}}::text, '{{ null_value_placeholder }}'))
                  {%- if not loop.last -%} || '{{ delimeter }}' ||  {%- endif -%}
                {%- endfor %},
                '{{ delimeter }}'
            ),
        '(' || concat('{{ delimeter }}', '{{ null_value_placeholder }}') || ')*$',
        ''
      ))
    {%- endset -%}
    {%- do return(hashdiff) -%}
{%- endmacro -%}
