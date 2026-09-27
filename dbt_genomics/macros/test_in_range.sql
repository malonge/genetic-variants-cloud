{% test in_range(model, column_name, min_val, max_val) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND ({{ column_name }} < {{ min_val }} OR {{ column_name }} > {{ max_val }})

{% endtest %}
