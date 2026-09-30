WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATE_TRUNC('WEEK', start_date) AS start_date_period,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM subscriptions
    WHERE end_date > '2023-03-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(days_end_date_to_start_date) OVER w_grp AS prev_days_end_date_to_start_date,
        LEAD(days_end_date_to_start_date) OVER w_grp AS next_days_end_date_to_start_date,
        LAG(monthly_fee) OVER w_grp AS prev_monthly_fee,
        ROW_NUMBER() OVER w_grp AS seq_no,
        LAG(days_end_date_to_start_date, 2) OVER (ORDER BY monthly_fee, id) AS prev_days_end_date_to_start_date_2
    FROM filtered
    WHERE start_date_period >= '2023-08-02'
    WINDOW w_grp AS (PARTITION BY status ORDER BY end_date, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date,
        start_date_period,
        days_end_date_to_start_date,
        prev_days_end_date_to_start_date,
        prev_monthly_fee,
        seq_no,
        LAG(prev_days_end_date_to_start_date, 1) OVER (PARTITION BY status ORDER BY prev_days_end_date_to_start_date_2, id) AS prev_prev_days_end_date_to_start_date,
        SUM(days_end_date_to_start_date) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_end_date_to_start_date,
        NTILE(10) OVER (ORDER BY days_end_date_to_start_date, id) AS days_end_date_to_start_date_bucket
    FROM scored
),
scored_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date_period,
        days_end_date_to_start_date,
        prev_days_end_date_to_start_date,
        seq_no,
        days_end_date_to_start_date_bucket,
        AVG(running_days_end_date_to_start_date) OVER w_grp AS running_days_end_date_to_start_date_avg_6_prior,
        DENSE_RANK() OVER (ORDER BY prev_days_end_date_to_start_date NULLS FIRST) AS prev_days_end_date_to_start_date_dense_rank,
        DENSE_RANK() OVER (ORDER BY prev_prev_days_end_date_to_start_date NULLS LAST) AS prev_prev_days_end_date_to_start_date_dense_rank
    FROM enriched
    WINDOW w_grp AS (ORDER BY days_end_date_to_start_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        days_end_date_to_start_date,
        running_days_end_date_to_start_date_avg_6_prior,
        prev_days_end_date_to_start_date_dense_rank,
        prev_prev_days_end_date_to_start_date_dense_rank,
        seq_no + days_end_date_to_start_date AS seq_no_plus_days_end_date_to_start_date,
        seq_no + prev_prev_days_end_date_to_start_date_dense_rank AS seq_no_plus_prev_prev_days_end_date_to_start_date_dense_rank
    FROM scored_subscriptions
    WHERE prev_days_end_date_to_start_date_dense_rank >= 6
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN prev_prev_days_end_date_to_start_date_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_prev_days_end_date_to_start_date_dense_rank_tier,
        CASE WHEN running_days_end_date_to_start_date_avg_6_prior >= 308 THEN 'LARGE' ELSE 'SMALL' END AS running_days_end_date_to_start_date_avg_6_prior_tier,
        PERCENTILE_APPROX(prev_days_end_date_to_start_date_dense_rank, 0.5) AS prev_days_end_date_to_start_date_dense_rank_p50,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(running_days_end_date_to_start_date_avg_6_prior, 0.95) AS running_days_end_date_to_start_date_avg_6_prior_p95
    FROM cleaned
    GROUP BY CUBE (CASE WHEN prev_prev_days_end_date_to_start_date_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_days_end_date_to_start_date_avg_6_prior >= 308 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    prev_prev_days_end_date_to_start_date_dense_rank_tier,
    running_days_end_date_to_start_date_avg_6_prior_tier,
    distinct_ids,
    running_days_end_date_to_start_date_avg_6_prior_p95
FROM daily
ORDER BY prev_prev_days_end_date_to_start_date_dense_rank_tier DESC NULLS FIRST
