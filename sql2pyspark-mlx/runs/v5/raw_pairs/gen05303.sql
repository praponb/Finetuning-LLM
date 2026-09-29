WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        LAG(monthly_fee) OVER w_grp AS prev_monthly_fee,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM subscriptions
    WINDOW w_grp AS (PARTITION BY plan, status ORDER BY start_date ASC, id ASC)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        end_date,
        prev_monthly_fee,
        row_seq,
        COUNT(prev_monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_prev_monthly_fee,
        SUM(prev_monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_prev_monthly_fee,
        MIN(prev_monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_prev_monthly_fee,
        MAX(prev_monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_prev_monthly_fee
    FROM scored
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        end_date,
        running_count_prev_monthly_fee,
        running_max_prev_monthly_fee,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY running_max_prev_monthly_fee ASC, id ASC) AS running_max_prev_monthly_fee_bucket,
        ROUND(running_min_prev_monthly_fee / SUM(running_min_prev_monthly_fee) OVER (PARTITION BY customer_id), 4) AS running_min_prev_monthly_fee_share
    FROM metrics
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.running_max_prev_monthly_fee,
        s.running_max_prev_monthly_fee_bucket,
        b.city,
        b.age
    FROM with_windows s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(running_max_prev_monthly_fee_bucket) OVER (PARTITION BY city ORDER BY running_max_prev_monthly_fee_bucket ASC, id ASC) AS prev_running_max_prev_monthly_fee_bucket,
        LEAD(age) OVER (PARTITION BY city ORDER BY running_max_prev_monthly_fee_bucket ASC, id ASC) AS next_age,
        LAG(age) OVER (PARTITION BY city ORDER BY running_max_prev_monthly_fee_bucket ASC, id ASC) AS prev_age,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY running_max_prev_monthly_fee_bucket ASC, id ASC) AS seq_no,
        SUM(running_max_prev_monthly_fee_bucket) OVER (PARTITION BY city, customer_id ORDER BY running_max_prev_monthly_fee_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_max_prev_monthly_fee_bucket
    FROM joined
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        running_max_prev_monthly_fee,
        running_max_prev_monthly_fee_bucket,
        city,
        age,
        next_age,
        seq_no,
        cumulative_running_max_prev_monthly_fee_bucket,
        CAST(prev_running_max_prev_monthly_fee_bucket AS DOUBLE) AS prev_running_max_prev_monthly_fee_bucket_num,
        CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END AS is_berlin
    FROM scored_subscriptions
    WHERE seq_no = 4
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.age,
        src.seq_no,
        src.cumulative_running_max_prev_monthly_fee_bucket,
        c.signup_date
    FROM valid src
    LEFT JOIN customers c
        ON src.customer_id <=> c.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN seq_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_tier,
        CASE WHEN cumulative_running_max_prev_monthly_fee_bucket >= 38 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_running_max_prev_monthly_fee_bucket_tier,
        DATE_TRUNC('WEEK', signup_date) AS week_start,
        COUNT(*) AS cnt,
        ROUND(AVG(seq_no), 2) AS avg_seq_no,
        COUNT(*) AS cnt_2,
        MIN(seq_no) AS min_seq_no,
        AVG(cumulative_running_max_prev_monthly_fee_bucket) AS avg_cumulative_running_max_prev_monthly_fee_bucket,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN seq_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY AVG(cumulative_running_max_prev_monthly_fee_bucket) DESC) AS avg_cumulative_running_max_prev_monthly_fee_bucket_dense_rank
    FROM with_dims
    GROUP BY 1, 2, 3
    HAVING cnt > 1
)
SELECT
    seq_no_tier,
    cumulative_running_max_prev_monthly_fee_bucket_tier,
    week_start,
    avg_seq_no,
    min_seq_no,
    avg_cumulative_running_max_prev_monthly_fee_bucket,
    avg_cumulative_running_max_prev_monthly_fee_bucket_dense_rank
FROM totals
ORDER BY min_seq_no ASC, avg_seq_no DESC;
