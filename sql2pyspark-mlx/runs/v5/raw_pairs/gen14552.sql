WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY paid_at NULLS FIRST, id) AS rank_in_group
        FROM payments
    ) ranked
    WHERE rank_in_group = 1
),
valid AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        rank_in_group,
        ROUND(amount / rank_in_group, 2) AS amount_per_rank_in_group,
        COALESCE(method, 'UNKNOWN') AS method_filled
    FROM ranked
    WHERE paid_at > TIMESTAMP '2023-08-19 03:00:00'
      AND method IS NOT NULL
),
metrics AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        amount_per_rank_in_group,
        AVG(amount) OVER w_hist AS running_avg_amount,
        MIN(amount) OVER w_hist AS running_min_amount,
        COUNT(amount) OVER w_hist AS running_count_amount,
        SUM(amount) OVER w_hist AS running_sum_amount
    FROM valid
    WINDOW w_hist AS (PARTITION BY order_id, status ORDER BY amount_per_rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        id,
        method,
        amount_per_rank_in_group,
        running_avg_amount,
        running_sum_amount,
        SUM(running_avg_amount) OVER (PARTITION BY order_id ORDER BY amount_per_rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_avg_amount,
        LAG(amount, 1) OVER (PARTITION BY method ORDER BY running_avg_amount, id) AS prev_amount,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        ROUND(running_avg_amount / SUM(running_avg_amount) OVER (PARTITION BY method), 4) AS running_avg_amount_share
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY method ORDER BY running_count_amount, id)
),
summary AS (
    SELECT
        CASE WHEN prev_amount >= 1340 THEN 'HIGH' ELSE 'LOW' END AS prev_amount_tier,
        COUNT(*) AS cnt,
        AVG(running_avg_amount_share) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_avg_running_avg_amount_share,
        ROUND(SUM(running_avg_amount_share) / NULLIF(COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END), 0), 2) AS running_avg_amount_share_per_bank_transfer
    FROM enriched
    GROUP BY prev_amount_tier
)
SELECT prev_amount_tier, bank_transfer_avg_running_avg_amount_share
FROM summary
