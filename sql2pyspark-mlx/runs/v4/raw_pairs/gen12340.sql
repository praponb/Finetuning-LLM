WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        amount,
        status,
        ROUND(amount * 1.07, 2) amount_scaled,
        CEIL(amount) amount_whole,
        COALESCE(method, 'NONE') method_clean
    FROM payments
    WHERE order_id IS NOT NULL
      AND order_id = 27
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY status ORDER BY amount ASC, id ASC) amount_bucket,
        SUM(amount) OVER (ORDER BY amount_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_amount,
        SUM(amount) OVER (PARTITION BY status ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_amount_2
    FROM filtered
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        o.discount,
        o.status AS order_status
    FROM with_windows s
    JOIN orders o
        ON s.order_id = o.id
)
SELECT
    id,
    amount,
    amount_scaled,
    amount_bucket,
    cumulative_amount,
    cumulative_amount_2,
    discount
FROM with_dims
