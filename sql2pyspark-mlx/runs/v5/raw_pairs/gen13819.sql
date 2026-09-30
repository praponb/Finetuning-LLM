WITH cleaned AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        DATE_FORMAT(started_at, 'yyyy-MM') started_at_label,
        DATE_TRUNC('week', started_at) started_at_period,
        COALESCE(country, 'OTHER') country_filled
    FROM sessions
    WHERE country IS NOT NULL
      AND country IN ('TH', 'JP')
)
SELECT country, browser
FROM cleaned
EXCEPT
SELECT country, browser
FROM sessions
WHERE TO_DATE(started_at) < '2025-03-01'
