WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        TRIM(browser) AS clean_browser,
        COALESCE(country, 'DIRECT') AS country_filled
    FROM sessions
    WHERE country = 'SG'
)
SELECT
    id,
    customer_id,
    browser,
    country,
    LAST_VALUE(country, TRUE) OVER (ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
    FIRST_VALUE(browser, TRUE) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id ASC) AS row_seq
FROM base
