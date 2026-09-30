WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        amount,
        merchant,
        is_flagged,
        MIN(amount) OVER w_hist AS running_min_amount,
        SUM(amount) OVER w_hist AS running_sum_amount,
        MAX(amount) OVER w_hist AS running_max_amount
    FROM transactions
    WINDOW w_hist AS (PARTITION BY merchant, txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(running_max_amount) OVER (PARTITION BY merchant, account_id ORDER BY running_max_amount, id) AS prev_running_max_amount,
        LEAD(running_max_amount) OVER (PARTITION BY merchant, account_id ORDER BY running_max_amount, id) AS next_running_max_amount,
        LEAD(amount) OVER (PARTITION BY merchant, account_id ORDER BY running_max_amount, id) AS next_amount,
        LAG(amount) OVER (PARTITION BY merchant, account_id ORDER BY running_max_amount, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY merchant, account_id ORDER BY running_max_amount, id) AS event_seq
    FROM enriched
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM transactions
    WHERE amount >= 1600
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        amount,
        merchant,
        is_flagged,
        running_min_amount,
        running_sum_amount,
        running_max_amount,
        prev_running_max_amount,
        next_running_max_amount,
        next_amount,
        event_seq,
        amount * event_seq AS amount_times_event_seq
    FROM kept_rows
    WHERE next_running_max_amount < 3900
)
-- Multi-level aggregation
SELECT
    CASE WHEN next_running_max_amount >= 3200 THEN 'LARGE' ELSE 'SMALL' END AS next_running_max_amount_tier,
    COUNT_IF(merchant = 'Lazada') AS lazada_events,
    ROUND(SUM(next_amount) / NULLIF(COUNT(CASE WHEN merchant = 'Shell' THEN 1 END), 0), 2) AS next_amount_per_shell,
    ROUND(AVG(running_max_amount), 2) AS avg_running_max_amount
FROM base
GROUP BY CASE WHEN next_running_max_amount >= 3200 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_running_max_amount >= 0
ORDER BY next_running_max_amount_tier DESC NULLS LAST;
