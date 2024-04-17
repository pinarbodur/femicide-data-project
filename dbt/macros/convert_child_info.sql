{#
    Returns child info description
#}

{% macro convert_child_info(child_info) -%}

    case {{ child_info }}
        when 'Var' then 'yes'
        when 'Yok' then 'no'
        when 'Hamile' then 'pregnant'
    end

{%- endmacro %}