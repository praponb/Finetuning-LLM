WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date NULLS LAST, id) AS rn
        FROM accounts
    ) t
    WHERE rn = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM deduped src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date BETWEEN sub.start_date AND sub.end_date
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY subscription_id DESC, id, subscription_id) AS subscription_id_bucket,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY subscription_id DESC, id, subscription_id) AS row_seq,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY rn, id, subscription_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS balance_avg_2_prior
    FROM attributed
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY monthly_fee NULLS FIRST, id, subscription_id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
latest_accounts AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY status, customer_id ORDER BY rn DESC) = 1
)
-- Grouped metrics
SELECT
    subscription_id,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS monthly_fee_per_enterprise,
    AVG(subscription_id) AS avg_subscription_id
FROM latest_accounts
GROUP BY subscription_id;
