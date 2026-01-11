{% macro HACKATHON(value) %}
    -- Use BigQuery's SHA256 to generate a reproducible hash key
    TO_HEX(SHA256({{ value }}))
{% endmacro %}
