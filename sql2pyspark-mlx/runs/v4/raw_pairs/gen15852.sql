WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        CEIL(credit_limit) AS credit_limit_whole,
        LENGTH(country) AS country_length
    FROM customers
    WHERE signup_date > TO_DATE('2023-11-23')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(credit_limit_whole, 0) AS credit_limit_whole_filled,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM filtered
    WHERE signup_date >= DATE '2023-09-22'
      AND segment IS NOT NULL
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.discount,
        top1.order_order_date
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.discount, c.order_date AS order_order_date
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT segment, id
    FROM with_latest
    EXCEPT
    SELECT segment, id
    FROM customers
    WHERE credit_limit < 13400
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    JOIN kept_keys k ON s.segment = k.segment AND s.id = k.id
)
SELECT
    name,
    email,
    country,
    signup_date,
    is_active,
    credit_limit_whole,
    country_length,
    credit_limit_whole_filled,
    order_order_date
FROM verified
ORDER BY signup_date NULLS FIRST, country_length
