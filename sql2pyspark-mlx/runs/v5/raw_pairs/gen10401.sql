WITH cleaned AS (
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        COALESCE(balance, 0) balance_filled
    FROM accounts
    WHERE status IS DISTINCT FROM 'OPEN'
),
metrics AS (
    SELECT
        *,
        COUNT(balance) OVER w_ordered last3_count_balance,
        MIN(balance) OVER w_ordered last3_min_balance,
        AVG(balance) OVER w_ordered last3_avg_balance,
        MAX(balance) OVER w_ordered last3_max_balance,
        ROW_NUMBER() OVER w_part row_seq
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY customer_id, status ORDER BY balance_filled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY customer_id ORDER BY balance, id)
),
enriched AS (
    SELECT
        id,
        customer_id,
        balance,
        balance_filled,
        last3_avg_balance,
        last3_max_balance,
        LEAD(last3_min_balance, 1) OVER (PARTITION BY customer_id ORDER BY last3_max_balance, id) next_last3_min_balance,
        SUM(last3_avg_balance) OVER (PARTITION BY customer_id ORDER BY balance_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_last3_avg_balance,
        LAST_VALUE(last3_max_balance) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY last3_min_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_last3_max_balance
    FROM metrics
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY running_last3_avg_balance DESC, id) AS rank_in_group
        FROM enriched
    ) t
    WHERE rank_in_group = 1
),
base AS (
    SELECT
        id,
        customer_id,
        balance,
        last3_max_balance,
        next_last3_min_balance,
        running_last3_avg_balance,
        rank_in_group,
        CASE WHEN last_known_last3_max_balance >= 71000 THEN 'High' WHEN last_known_last3_max_balance >= 13000 THEN 'Medium' ELSE 'Low' END last_known_last3_max_balance_tier
    FROM deduped
    WHERE customer_id IS NOT NULL
)
SELECT
    id,
    customer_id,
    balance,
    running_last3_avg_balance,
    rank_in_group,
    last_known_last3_max_balance_tier
FROM base
