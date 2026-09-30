WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(amount AS INT) AS amount_int
    FROM payments
    WHERE paid_at >= DATE '2024-05-27'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(amount_int, 1) OVER w_ordered AS prev_amount_int,
        AVG(amount_int) OVER w_part AS amount_int_avg_2,
        AVG(amount) OVER w_recent AS amount_avg_90d,
        LAG(amount_int, 1) OVER (PARTITION BY status ORDER BY amount, id) AS prev_amount_int_2
    FROM valid
    WINDOW w_ordered AS (PARTITION BY order_id, method ORDER BY amount_int, id), w_part AS (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END AS is_bank_transfer
    FROM scored
    WHERE paid_at >= TO_TIMESTAMP('2024-04-15 04:00:00')
      AND prev_amount_int = 65
)
-- Grouped metrics
SELECT
    CASE WHEN amount_avg_90d >= 1714 THEN 'HIGH' ELSE 'LOW' END AS amount_avg_90d_tier,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    COUNT(DISTINCT order_id) AS distinct_order_ids_2,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN amount_avg_90d >= 1714 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(DISTINCT order_id) DESC) AS distinct_order_ids_dense_rank
FROM base
GROUP BY CASE WHEN amount_avg_90d >= 1714 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', paid_at);
