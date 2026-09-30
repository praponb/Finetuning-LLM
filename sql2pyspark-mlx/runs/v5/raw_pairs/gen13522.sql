WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        CONCAT_WS('-', name, city) AS name_city_key
    FROM customers
    WHERE signup_date >= DATE '2023-02-18'
),
latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.segment,
        s.is_active,
        s.age,
        x.monthly_fee,
        x.end_date
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.monthly_fee, c.end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        country,
        city,
        is_active,
        monthly_fee,
        end_date,
        UPPER(segment) AS segment_upper,
        INITCAP(TRIM(city)) AS city_title
    FROM latest_match
    WHERE city IS NOT NULL
      AND segment IS NOT DISTINCT FROM 'Consumer'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date < s.end_date
        )
)
SELECT
    id,
    country,
    city,
    is_active,
    city_title,
    CASE WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'CORE' ELSE 'GROUP_A' END AS city_zone
FROM kept;
