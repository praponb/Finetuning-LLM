WITH cleaned AS (
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        DATE_TRUNC('YEAR', started_at) started_at_period,
        INITCAP(TRIM(country)) country_title,
        COALESCE(country, 'OTHER') country_filled
    FROM sessions
    WHERE country IS NOT NULL
      AND browser IS DISTINCT FROM 'Edge'
)
SELECT
    id,
    customer_id,
    device,
    started_at,
    country_title,
    country_filled
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at_period DESC NULLS FIRST, id) = 1
