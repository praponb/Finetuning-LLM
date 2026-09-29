WITH base AS (
    SELECT *, CAST(amount AS INT) AS amount_int
    FROM orders
    WHERE order_date > DATE '2023-08-14'
      AND channel IS NOT NULL
),
latest AS (
    SELECT
        id,
        store_id,
        order_date,
        channel,
        amount
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) = 1
),
filtered AS (
    SELECT
        id,
        store_id,
        order_date,
        channel,
        amount,
        SUBSTRING(channel, 1, 4) AS channel_prefix,
        CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END AS is_mobile
    FROM latest
    WHERE channel IS DISTINCT FROM 'web'
      AND amount < 1440
),
metrics AS (
    SELECT
        id,
        order_date,
        amount,
        channel_prefix,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY amount DESC NULLS LAST) AS amount_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY amount NULLS FIRST) AS amount_percent_rank,
        ROUND(amount / SUM(amount) OVER (PARTITION BY store_id), 4) AS amount_share,
        SUM(amount) OVER (PARTITION BY store_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY store_id, channel), 4) AS amount_share_2
    FROM filtered
    WHERE is_mobile <= 30
),
kept AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.product_id IS NOT NULL
        )
)
SELECT *
FROM (
    SELECT
        id,
        amount_percent_rank,
        amount
    FROM kept
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (amount_percent_rank, amount AS AMOUNT)
);
