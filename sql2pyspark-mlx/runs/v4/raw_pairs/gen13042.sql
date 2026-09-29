WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.age,
        d.email,
        d.name
    FROM flattened a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        qty,
        price,
        promo,
        age,
        email,
        LENGTH(email) AS email_length,
        price - product_id AS price_minus_product_id
    FROM with_dims
    WHERE created_at >= DATE '2025-03-01'
      AND promo IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM cleaned
    EXCEPT
    SELECT customer_id, id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
totals AS (
    -- Grouped metrics
    SELECT
        customer_id,
        DATE_TRUNC('HOUR', created_at) AS hour_window,
        COUNT_IF(promo = 'SPRING10') AS spring10_events,
        COUNT(*) AS cnt,
        APPROX_PERCENTILE(email_length, ARRAY(0.25, 0.5, 0.75)) AS email_length_quartiles
    FROM kept_rows
    GROUP BY GROUPING SETS ((customer_id, DATE_TRUNC('HOUR', created_at)), (customer_id), ())
    HAVING spring10_events > 1
)
SELECT
    s.customer_id,
    s.hour_window,
    s.cnt,
    s.email_length_quartiles,
    (SELECT MAX(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS max_account_balance
FROM totals s;
