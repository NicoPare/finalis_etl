{{ config(
    materialized='incremental',
    unique_key='id_hash',
    tags=['clean', 'federal_reserve']
) }}

SELECT
    id_hash,
    series_id,
    raw_json::JSON->>'date' AS "date",
    raw_json::JSON->>'value' AS "value",
    loaded_at
FROM {{ source('raw', 'DFF') }}
{% if is_incremental() %}
WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %}
