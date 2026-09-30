WITH enriched AS (
    SELECT
        *,
        SUM(amount) OVER w_hist AS running_sum_amount,
        COUNT(amount) OVER w_hist AS running_count_amount,
        MIN(amount) OVER w_hist AS running_min_amount,
        MAX(amount) OVER w_hist AS running_max_amount
    FROM orders
    WHERE amount >= 1250
    WINDOW w_hist AS (PARTITION BY status ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    SELECT
        id,
        customer_id,
        order_date,
        amount,
        shipping_fee,
        running_sum_amount,
        running_max_amount
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, id) AS dedup_rank
        FROM enriched
    ) x
    WHERE dedup_rank = 1
),
kept_keys AS (
    SELECT id, customer_id
    FROM deduped
    EXCEPT ALL
    SELECT id, customer_id
    FROM orders
    WHERE customer_id IN (9, 20, 23)
),
kept_rows AS (
    SELECT s.*
    FROM deduped s
    JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
not_flagged AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.unit_price BETWEEN 230 AND 380
        )
),
not_flagged_orders AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date < s.order_date
        )
),
joined AS (
    SELECT
        e.customer_id,
        e.shipping_fee,
        c.signup_date,
        c.credit_limit,
        c.country
    FROM not_flagged_orders e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
filtered AS (
    SELECT
        customer_id,
        shipping_fee,
        signup_date,
        credit_limit,
        DATE_TRUNC('YEAR', signup_date) AS signup_date_period,
        COALESCE(country, 'N/A') AS country_filled
    FROM joined
    WHERE country <> 'JP'
),
totals AS (
    SELECT
        DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
        CASE WHEN shipping_fee >= 44 THEN 'LARGE' ELSE 'SMALL' END AS shipping_fee_tier,
        ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
        COUNT(*) AS row_count
    FROM filtered
    GROUP BY DATE_TRUNC('QUARTER', signup_date), CASE WHEN shipping_fee >= 44 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    quarter_start,
    shipping_fee_tier,
    avg_shipping_fee,
    row_count
FROM totals
ORDER BY quarter_start NULLS FIRST;
