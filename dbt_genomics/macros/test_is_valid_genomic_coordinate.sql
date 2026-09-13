{% test is_valid_genomic_coordinate(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} <= 0
   OR {{ column_name }} > 300000000

{% endtest %}
