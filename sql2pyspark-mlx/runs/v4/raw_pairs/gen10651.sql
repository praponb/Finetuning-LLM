WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY end_date, id) AS rank_in_group
        FROM subscriptions
    ) ranked
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY rank_in_group, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_2_prior,
        LEAD(monthly_fee, 2) OVER (PARTITION BY customer_id, status ORDER BY rank_in_group, id) AS next_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status, plan), 4) AS monthly_fee_share
    FROM deduped
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        rank_in_group,
        monthly_fee_avg_2_prior,
        next_monthly_fee,
        monthly_fee_share,
        LAG(monthly_fee_share, 1) OVER (PARTITION BY status ORDER BY monthly_fee, id) AS prev_monthly_fee_share,
        MAX(monthly_fee) OVER (PARTITION BY status) AS status_max_monthly_fee,
        FIRST_VALUE(monthly_fee_avg_2_prior) IGNORE NULLS OVER (ORDER BY monthly_fee_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee_avg_2_prior,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status, customer_id), 4) AS monthly_fee_share_2
    FROM with_windows
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.end_date,
    s.monthly_fee,
    s.rank_in_group,
    s.monthly_fee_avg_2_prior,
    s.next_monthly_fee,
    s.prev_monthly_fee_share,
    (SELECT MIN(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS min_account_balance
FROM enriched s
ORDER BY monthly_fee_avg_2_prior DESC NULLS LAST;
