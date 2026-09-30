WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, discount * amount AS discount_times_amount
    FROM orders
    WHERE order_date > DATE '2023-11-17'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
        )
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM eligible src
    INNER JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date < pr.ends_on + INTERVAL 30 DAYS
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY promo_id DESC, id, promo_id) AS rn
        FROM attributed
    ) x
    WHERE rn = 1
)
SELECT
    id,
    store_id,
    order_date,
    amount,
    discount,
    shipping_fee,
    promo_id,
    promo_code,
    discount_pct
FROM ranked;
