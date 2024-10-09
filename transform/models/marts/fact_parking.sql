WITH parking_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['st_park_p.exploit']) }} AS exploit_id,
        {{ dbt_utils.generate_surrogate_key(['st_park_p.secteur']) }} AS secteur_id,
        {{ dbt_utils.generate_surrogate_key(['st_park_p.ta_type']) }} AS ta_type_id,
        {{ dbt_utils.generate_surrogate_key(['st_park_p.type']) }} AS type_id,
        np_total,
        np_pmr,
        np_vle,
        th_heur,
        tv_1h
    FROM {{ source('staging', 'st_park_p') }}
)

SELECT
    dim_exploit.exploit_id,
    dim_secteur.secteur_id,
    dim_ta_type.ta_type_id,
    dim_type.type_id,
    SUM(parking_data.np_total) AS somme_np_total,
    SUM(parking_data.np_pmr) AS somme_np_pmr,
    SUM(parking_data.np_vle) AS somme_np_vle,
    AVG(parking_data.th_heur) AS moyenne_th_heur,
    AVG(parking_data.tv_1h) AS moyenne_tv_1h
FROM parking_data
LEFT JOIN {{ ref('dim_exploit') }} AS dim_exploit
    ON parking_data.exploit_id = dim_exploit.exploit_id
LEFT JOIN {{ ref('dim_secteur') }} AS dim_secteur
    ON parking_data.secteur_id = dim_secteur.secteur_id
LEFT JOIN {{ ref('dim_ta_type') }} AS dim_ta_type
    ON parking_data.ta_type_id = dim_ta_type.ta_type_id
LEFT JOIN {{ ref('dim_type') }} AS dim_type
    ON parking_data.type_id = dim_type.type_id
GROUP BY
    dim_exploit.exploit_id,
    dim_secteur.secteur_id,
    dim_ta_type.ta_type_id,
    dim_type.type_id
