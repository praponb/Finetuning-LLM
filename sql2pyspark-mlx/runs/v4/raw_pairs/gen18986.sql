WITH joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.credit_limit,
        b.segment,
        b.country
    FROM subscriptions s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY credit_limit DESC) AS rank_in_group
        FROM joined
    ) ranked
    WHERE rank_in_group = 1
),
joined_subscriptions AS (
    -- Attach customers attributes
    SELECT e.*, d.credit_limit AS customer_credit_limit
    FROM ranked e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM joined_subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY start_date DESC NULLS FIRST, id ASC) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        start_date,
        end_date,
        status,
        segment,
        country,
        customer_credit_limit,
        DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date
    FROM latest
    WHERE plan IN ('premium', 'basic', 'enterprise')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY days_since_start_date DESC NULLS LAST) AS days_since_start_date_dense_rank,
        FIRST_VALUE(segment, TRUE) OVER (PARTITION BY country ORDER BY days_since_start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_segment,
        NTILE(3) OVER (PARTITION BY status ORDER BY days_since_start_date DESC, id ASC) AS days_since_start_date_bucket,
        LAST_VALUE(segment) IGNORE NULLS OVER (PARTITION BY status ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_segment,
        LAG(days_since_start_date, 2) OVER w_recent AS prev_days_since_start_date
    FROM base
    WINDOW w_recent AS (PARTITION BY segment ORDER BY customer_credit_limit ASC, id ASC)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        status,
        DATE_TRUNC('WEEK', end_date) AS week_start,
        CASE WHEN days_since_start_date_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS days_since_start_date_bucket_tier,
        ROUND(AVG(days_since_start_date_dense_rank), 2) AS avg_days_since_start_date_dense_rank,
        APPROX_PERCENTILE(days_since_start_date_dense_rank, 0.5) AS days_since_start_date_dense_rank_p50,
        COUNT(*) FILTER (WHERE status = 'PAUSED') AS paused_rows,
        SUM(CASE WHEN status = 'EXPIRED' THEN prev_days_since_start_date ELSE 0 END) AS expired_prev_days_since_start_date,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY APPROX_PERCENTILE(days_since_start_date_dense_rank, 0.5) DESC) AS days_since_start_date_dense_rank_p50_percent_rank
    FROM metrics
    GROUP BY status, DATE_TRUNC('WEEK', end_date), CASE WHEN days_since_start_date_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    status,
    week_start,
    days_since_start_date_bucket_tier,
    avg_days_since_start_date_dense_rank,
    days_since_start_date_dense_rank_p50,
    expired_prev_days_since_start_date
FROM stats
ORDER BY days_since_start_date_dense_rank_p50 ASC;
