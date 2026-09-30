WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        COALESCE(method, 'Unknown') AS method_filled,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2023-06-21 14:00:00')
      AND method IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        method_filled,
        method_clean,
        COALESCE(amount, 0) AS amount_filled,
        ROUND(amount * 1.15, 2) AS adjusted_amount
    FROM valid
    WHERE paid_at >= TO_TIMESTAMP('2023-09-22 13:00:00')
      AND order_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(amount_filled) OVER (PARTITION BY order_id ORDER BY adjusted_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount_filled,
        MIN(amount_filled) OVER (PARTITION BY order_id ORDER BY adjusted_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount_filled,
        SUM(amount_filled) OVER (PARTITION BY order_id ORDER BY adjusted_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount_filled,
        MAX(adjusted_amount) OVER (PARTITION BY status, order_id ORDER BY CAST(paid_at AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS adjusted_amount_max_90d
    FROM filtered
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        method_filled,
        method_clean,
        amount_filled,
        running_max_amount_filled,
        running_min_amount_filled,
        running_sum_amount_filled,
        AVG(running_max_amount_filled) OVER (PARTITION BY status ORDER BY running_min_amount_filled ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_max_amount_filled_avg_3,
        SUM(amount) OVER (PARTITION BY status, order_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        PERCENT_RANK() OVER w_grp AS running_sum_amount_filled_percent_rank
    FROM scored
    WHERE amount >= 720
    WINDOW w_grp AS (PARTITION BY status ORDER BY running_sum_amount_filled ASC NULLS FIRST)
)
SELECT
    id,
    order_id,
    amount,
    status,
    method_filled,
    method_clean,
    amount_filled,
    running_max_amount_filled,
    running_min_amount_filled,
    CASE WHEN status RLIKE '^(SUCCESS|REFUNDED)$' THEN 'CORE' WHEN status RLIKE '^(FAILED)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS status_zone
FROM enriched;
