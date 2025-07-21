{{ config(
    materialized='incremental',
    unique_key='observation_date',
    tags=['datawarehouse', 'federal_reserve']
) }}

WITH base AS (
    {% set models = [
        ("unrate_clean", "Unemployment Rate"),
        ("bamlcc0a0cmtriv_clean", "US Corporate Bond Index"),
        ("bamlhyh0a0hym2triv_clean", "US High Yield Bond Index"),
        ("nasdaq100_clean", "NASDAQ100 Index"),
        ("sp500_clean", "S&P 500 Index"),
        ("willmicrocappr_clean", "Wilshire Microcap Index"),
        ("dff_clean", "Federal Funds Effective Rate"),
        ("cpaltt01usm659n_clean", "Consumer Price Index"),
        ("vixcls_clean", "CBOE VIX")
    ] %}

    {% for model, label in models %}
    SELECT
        CAST("date" AS DATE) AS observation_date,
        CAST("value" AS FLOAT) AS value,
        '{{ label }}' AS metric
    FROM {{ ref(model) }}
    WHERE value != '.'
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

SELECT
    observation_date,
    MAX(CASE WHEN metric = 'Unemployment Rate' THEN value END) AS "Unemployment Rate",
    MAX(CASE WHEN metric = 'US Corporate Bond Index' THEN value END) AS "US Corporate Bond Index",
    MAX(CASE WHEN metric = 'US High Yield Bond Index' THEN value END) AS "US High Yield Bond Index",
    MAX(CASE WHEN metric = 'NASDAQ100 Index' THEN value END) AS "NASDAQ100 Index",
    MAX(CASE WHEN metric = 'S&P 500 Index' THEN value END) AS "S&P 500 Index",
    MAX(CASE WHEN metric = 'Wilshire Microcap Index' THEN value END) AS "Wilshire Microcap Index",
    MAX(CASE WHEN metric = 'Federal Funds Effective Rate' THEN value END) AS "Federal Funds Effective Rate",
    MAX(CASE WHEN metric = 'Consumer Price Index' THEN value END) AS "Consumer Price Index",
    MAX(CASE WHEN metric = 'CBOE VIX' THEN value END) AS "CBOE VIX"
FROM base
GROUP BY observation_date
