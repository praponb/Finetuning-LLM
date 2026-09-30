WITH remaining_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM subscriptions
    EXCEPT DISTINCT
    SELECT status, id
    FROM subscriptions
    WHERE start_date <= '2025-09-12'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM subscriptions s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT *, MAX(monthly_fee) OVER (PARTITION BY status) AS status_max_monthly_fee
    FROM kept_rows
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY monthly_fee DESC) AS monthly_fee_dense_rank,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY status_max_monthly_fee DESC) AS status_max_monthly_fee_dense_rank,
        DENSE_RANK() OVER (ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_dense_rank_2,
        SUM(status_max_monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_sum_status_max_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id, status), 4) AS monthly_fee_share
    FROM enriched
),
enriched_subscriptions AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        status_max_monthly_fee,
        monthly_fee_dense_rank_2,
        customer_id_sum_status_max_monthly_fee,
        monthly_fee_share,
        MIN(monthly_fee) OVER (PARTITION BY status) AS status_min_monthly_fee,
        ROUND(status_max_monthly_fee_dense_rank / SUM(status_max_monthly_fee_dense_rank) OVER (PARTITION BY status), 4) AS status_max_monthly_fee_dense_rank_share,
        ROUND(status_max_monthly_fee / SUM(status_max_monthly_fee) OVER (PARTITION BY plan), 4) AS status_max_monthly_fee_share,
        MIN(customer_id_sum_status_max_monthly_fee) OVER (PARTITION BY plan, customer_id) AS plan_min_customer_id_sum_status_max_monthly_fee,
        MIN(customer_id_sum_status_max_monthly_fee) OVER (PARTITION BY customer_id, status) AS customer_id_min_customer_id_sum_status_max_monthly_fee
    FROM metrics
    WHERE monthly_fee_dense_rank = 5
),
enriched_subscriptions_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        status_max_monthly_fee,
        monthly_fee_share,
        status_min_monthly_fee,
        status_max_monthly_fee_dense_rank_share,
        status_max_monthly_fee_share,
        customer_id_min_customer_id_sum_status_max_monthly_fee,
        ROUND(status_max_monthly_fee / SUM(status_max_monthly_fee) OVER (PARTITION BY customer_id), 4) AS status_max_monthly_fee_share_2,
        RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee_share NULLS LAST) AS monthly_fee_share_rank,
        COUNT(status_min_monthly_fee) OVER w_hist AS customer_id_count_status_min_monthly_fee
    FROM enriched_subscriptions
    WINDOW w_hist AS (PARTITION BY customer_id)
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.status_min_monthly_fee,
        a.monthly_fee_share_rank,
        a.customer_id_count_status_min_monthly_fee,
        b.credit_limit,
        b.segment,
        b.country
    FROM enriched_subscriptions_2 a
    LEFT JOIN customers b
        ON a.customer_id = b.id
)
-- Join a dimension and aggregate
SELECT
    c.segment AS customer_segment,
    src.country AS subscription_country,
    COUNT(*) AS num_rows,
    MAX(src.status_min_monthly_fee) AS max_status_min_monthly_fee
FROM joined src
INNER JOIN customers c ON src.customer_id = c.id
GROUP BY GROUPING SETS ((c.segment, src.country), (c.segment), ())
