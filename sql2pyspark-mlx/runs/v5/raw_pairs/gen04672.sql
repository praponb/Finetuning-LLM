WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        channel,
        discount,
        ADD_MONTHS(order_date, 1) AS order_date_plus_months,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM orders
    WHERE order_date >= DATE '2023-06-20'
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.channel_clean,
        d.city,
        d.region
    FROM cleaned s
    INNER JOIN stores d
        ON s.store_id = d.id
    WHERE d.store_name LIKE 'R%'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(city, 1, 4) AS city_prefix,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM with_dims
    WHERE region = 'East'
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.city_prefix,
        x.payment_status,
        x.payment_method
    FROM valid s
    JOIN LATERAL (
            SELECT c.status AS payment_status, c.method AS payment_method
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        city_prefix,
        payment_status,
        payment_method,
        CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END AS is_success
    FROM with_latest
    WHERE city IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT
    SELECT id
    FROM orders
    WHERE store_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    city,
    city_prefix,
    is_success,
    CASE WHEN city RLIKE '^(Berlin|Singapore)$' THEN 'CORE' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS city_zone
FROM kept_rows
ORDER BY city DESC NULLS FIRST, city_zone
