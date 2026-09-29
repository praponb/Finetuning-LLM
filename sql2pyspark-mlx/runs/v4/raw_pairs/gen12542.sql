WITH ranked AS (
    SELECT *
    FROM customers
    QUALIFY RANK() OVER (PARTITION BY city, country ORDER BY age) = 1
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY signup_date, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
metrics AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        latest_rank,
        SUM(credit_limit) OVER (ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country), 4) AS credit_limit_share,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS event_seq
    FROM deduped
),
scored AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        latest_rank,
        cumulative_credit_limit,
        credit_limit_share,
        event_seq,
        NTILE(4) OVER (PARTITION BY country, city ORDER BY credit_limit_share, id) AS credit_limit_share_bucket,
        LAST_VALUE(event_seq, TRUE) OVER w_ordered AS last_known_event_seq,
        RANK() OVER (PARTITION BY city, country ORDER BY event_seq NULLS LAST) AS event_seq_rank,
        LAST_VALUE(event_seq, TRUE) OVER w_ordered_2 AS last_known_event_seq_2
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY country ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered_2 AS (PARTITION BY country ORDER BY cumulative_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    country,
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    CASE WHEN last_known_event_seq_2 >= 9 THEN 'HIGH' ELSE 'LOW' END AS last_known_event_seq_2_tier,
    COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    PERCENTILE_APPROX(event_seq_rank, 0.5) AS event_seq_rank_p50
FROM scored
GROUP BY 1, 2, 3
