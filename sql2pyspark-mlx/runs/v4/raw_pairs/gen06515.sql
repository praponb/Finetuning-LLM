WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('MONTH', started_at) AS started_at_period
    FROM sessions
    WHERE browser NOT IN ('Firefox', 'Edge', 'Safari')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        browser,
        country,
        started_at_period,
        CONCAT(country, ' - ', device) AS country_device
    FROM filtered
    WHERE started_at >= TIMESTAMP '2023-03-25 06:00:00'
      AND browser IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY started_at_period DESC, id ASC) = 1
)
SELECT
    id,
    browser,
    country,
    country_device
FROM ranked
ORDER BY id DESC, browser ASC
