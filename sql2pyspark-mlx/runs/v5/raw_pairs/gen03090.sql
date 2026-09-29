WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(status) AS clean_status
    FROM subscriptions
    WHERE end_date > DATE '2023-08-17'
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.plan,
        src.monthly_fee,
        src.status,
        src.clean_status,
        c.name,
        c.age,
        c.city
    FROM cleaned src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, status ORDER BY monthly_fee DESC, id) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, customer_id
    FROM latest
    EXCEPT
    SELECT status, customer_id
    FROM subscriptions
    WHERE status IN ('CANCELLED', 'PAUSED', 'EXPIRED')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.status = k.status AND s.customer_id = k.customer_id
)
SELECT
    id,
    monthly_fee,
    clean_status,
    age,
    city,
    row_num,
    CASE WHEN city RLIKE '^(London|Singapore)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS city_zone
FROM reconciled
ORDER BY city NULLS FIRST;
