WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY start_date DESC NULLS FIRST, id ASC) AS row_num
        FROM subscriptions
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        row_num,
        COUNT(monthly_fee) OVER w_ordered AS status_count_monthly_fee,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee ASC NULLS FIRST) AS monthly_fee_dense_rank,
        SUM(monthly_fee) OVER (ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY status)
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        row_num,
        status_count_monthly_fee,
        monthly_fee_dense_rank,
        cumulative_monthly_fee,
        MIN(monthly_fee) OVER (PARTITION BY status) AS status_min_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY start_date ASC, id ASC) AS event_seq,
        ROUND(status_count_monthly_fee / SUM(status_count_monthly_fee) OVER (PARTITION BY plan, status), 4) AS status_count_monthly_fee_share
    FROM scored
),
scored_subscriptions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        row_num,
        status_count_monthly_fee,
        monthly_fee_dense_rank,
        cumulative_monthly_fee,
        status_min_monthly_fee,
        event_seq,
        status_count_monthly_fee_share,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share,
        SUM(event_seq) OVER (PARTITION BY status ORDER BY status_count_monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_seq
    FROM scored_subscriptions
)
-- Ranking and gap features
SELECT
    *,
    AVG(status_min_monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY monthly_fee_share ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS status_min_monthly_fee_avg_5,
    SUM(status_count_monthly_fee_share) OVER (PARTITION BY plan ORDER BY monthly_fee_dense_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_count_monthly_fee_share,
    STDDEV_SAMP(monthly_fee_dense_rank) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_monthly_fee_dense_rank,
    FIRST_VALUE(monthly_fee_share) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee_share,
    ROUND(cumulative_monthly_fee / SUM(cumulative_monthly_fee) OVER (PARTITION BY customer_id, status), 4) AS cumulative_monthly_fee_share
FROM scored_subscriptions_2
