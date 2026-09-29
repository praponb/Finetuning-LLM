WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(account_type) AS clean_account_type,
        LENGTH(status) AS status_length,
        COALESCE(status, 'DIRECT') AS status_clean
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND balance > 24000
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(status_length) OVER w_recent AS prev_status_length,
        LAG(balance) OVER w_recent AS prev_balance,
        LEAD(balance) OVER w_recent AS next_balance,
        ROW_NUMBER() OVER w_recent AS seq_no,
        NTILE(3) OVER w_recent_2 AS status_length_bucket
    FROM filtered
    WHERE status_length BETWEEN 36 AND 53
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY opened_date, id), w_recent_2 AS (PARTITION BY status ORDER BY status_length DESC, id)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_windows s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.opened_date >= sub.start_date
       AND s.opened_date < sub.end_date + INTERVAL 14 DAYS
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        balance,
        status,
        status_clean,
        next_balance,
        seq_no,
        subscription_id,
        plan,
        SUM(seq_no) OVER (PARTITION BY customer_id ORDER BY prev_status_length, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_seq_no,
        AVG(balance) OVER w_ordered AS balance_avg_6_prior,
        LAST_VALUE(subscription_id, TRUE) OVER (PARTITION BY plan, status ORDER BY status_length, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_subscription_id
    FROM matched
    WINDOW w_ordered AS (ORDER BY next_balance, id, subscription_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY subscription_id NULLS FIRST, id, subscription_id) AS row_num
        FROM scored
    ) t
    WHERE row_num = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(last_known_subscription_id) OVER w_grp AS last7_sum_last_known_subscription_id,
        MIN(last_known_subscription_id) OVER w_grp AS last7_min_last_known_subscription_id,
        AVG(last_known_subscription_id) OVER w_grp AS last7_avg_last_known_subscription_id
    FROM deduped
    WINDOW w_grp AS (PARTITION BY status ORDER BY seq_no, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    subscription_id,
    status,
    kpi,
    val
FROM (
    SELECT
        id,
        subscription_id,
        status,
        last7_avg_last_known_subscription_id,
        balance
    FROM enriched
) src
UNPIVOT (
    val FOR kpi IN (last7_avg_last_known_subscription_id, balance)
) u;
