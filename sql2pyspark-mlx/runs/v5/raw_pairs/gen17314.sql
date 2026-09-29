WITH cleaned AS (
    SELECT *, CEIL(credit_limit) AS credit_limit_whole
    FROM customers
    WHERE segment IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        credit_limit_whole,
        AVG(age) OVER (ORDER BY credit_limit_whole, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS age_avg_3_prior,
        RANK() OVER (PARTITION BY country, segment ORDER BY credit_limit_whole DESC) AS credit_limit_whole_rank
    FROM cleaned
),
metrics AS (
    SELECT
        id,
        name,
        country,
        signup_date,
        credit_limit_whole,
        credit_limit_whole_rank,
        CORR(credit_limit, age) OVER (PARTITION BY city, country) AS corr_credit_limit_age,
        NTILE(4) OVER (ORDER BY credit_limit_whole_rank DESC, id) AS credit_limit_whole_rank_bucket,
        STDDEV_SAMP(age_avg_3_prior) OVER (PARTITION BY segment) AS segment_stddev_samp_age_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit_whole DESC, id) AS event_seq,
        CORR(age, credit_limit) OVER (PARTITION BY country, city) AS corr_age_credit_limit
    FROM with_windows
),
metrics_customers AS (
    SELECT
        *,
        MAX(credit_limit_whole_rank) OVER w_recent AS running_max_credit_limit_whole_rank,
        SUM(credit_limit_whole_rank) OVER w_recent AS running_sum_credit_limit_whole_rank,
        AVG(credit_limit_whole_rank) OVER w_recent AS running_avg_credit_limit_whole_rank,
        ROUND(credit_limit_whole_rank / SUM(credit_limit_whole_rank) OVER (PARTITION BY country), 4) AS credit_limit_whole_rank_share
    FROM metrics
    WINDOW w_recent AS (PARTITION BY country ORDER BY credit_limit_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    SELECT
        s.id,
        s.credit_limit_whole_rank,
        s.credit_limit_whole_rank_bucket,
        s.segment_stddev_samp_age_avg_3_prior,
        s.event_seq,
        s.corr_age_credit_limit,
        s.running_sum_credit_limit_whole_rank,
        s.running_avg_credit_limit_whole_rank,
        s.credit_limit_whole_rank_share,
        latest.account_status
    FROM metrics_customers s
    JOIN LATERAL (
            SELECT c.status AS account_status
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest_customers AS (
    SELECT
        s.id,
        s.credit_limit_whole_rank_bucket,
        s.segment_stddev_samp_age_avg_3_prior,
        s.event_seq,
        s.credit_limit_whole_rank_share,
        s.account_status,
        x.session_device,
        x.session_country
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.device AS session_device, c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    SELECT
        id,
        credit_limit_whole_rank_share,
        session_country
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_device ORDER BY credit_limit_whole_rank_bucket, id) AS row_num
        FROM with_latest_customers
    ) x
    WHERE row_num = 1
)
SELECT
    CASE WHEN credit_limit_whole_rank_share >= 0.88 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_whole_rank_share_tier,
    APPROX_PERCENTILE(credit_limit_whole_rank_share, 0.5) AS credit_limit_whole_rank_share_p50,
    COUNT(*) FILTER (WHERE session_country = 'DE') AS de_rows,
    SUM(CASE WHEN session_country = 'TH' THEN credit_limit_whole_rank_share ELSE 0 END) AS th_credit_limit_whole_rank_share,
    PERCENTILE_APPROX(credit_limit_whole_rank_share, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_whole_rank_share_quartiles,
    COUNT(CASE WHEN session_country = 'US' THEN 1 END) AS us_count
FROM latest
GROUP BY 1;
