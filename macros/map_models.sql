{% macro map_models(apply_func) -%}

  {% if execute %}
    {% set ods_models = [] -%}  
    {% for node in graph.nodes.values() %}
      {%- if "run_map_macro" in node['tags'] 
          and "off_sync_meta" not in node['tags'] 
          and node['resource_type'] ==  "model"
      -%}
        {{ apply_func(node) }}
      {%- endif -%}
    {%- endfor %}
  {% endif %}

{%- endmacro %}