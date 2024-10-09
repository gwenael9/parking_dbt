SELECT
    {{ dbt_utils.generate_surrogate_key(['type']) }} AS type_id,
    "type" AS type_name
FROM {{ source('staging', 'st_park_p') }}
GROUP BY "type"