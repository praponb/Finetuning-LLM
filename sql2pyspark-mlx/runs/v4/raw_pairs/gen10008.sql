WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        country,
        INITCAP(TRIM(country)) AS country_title,
        SUBSTRING(country, 1, 4) AS country_prefix,
        COALESCE(country, 'OTHER') AS country_clean
    FROM sessions
    WHERE started_at >= TIMESTAMP '2024-02-17 14:00:00'
      AND browser IS NOT NULL
)
SELECT
    a.*,
    b.age,
    b.country AS customer_country
FROM base a
LEFT JOIN customers b
    ON a.customer_id = b.id;
