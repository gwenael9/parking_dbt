SELECT
    {{ dbt_utils.generate_surrogate_key(['secteur']) }} AS secteur_id,
    secteur AS secteur_name
FROM {{ ref('st_park_p') }}
GROUP BY secteur