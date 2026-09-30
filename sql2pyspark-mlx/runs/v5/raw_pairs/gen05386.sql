WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(is_active AS INT) is_active_flag
    FROM customers
    WHERE signup_date >= DATE '2023-06-28'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        AVG(age) OVER (PARTITION BY city) city_avg_age,
        NTILE(5) OVER (PARTITION BY country ORDER BY credit_limit, id) credit_limit_bucket,
        LAST_VALUE(segment) IGNORE NULLS OVER (PARTITION BY segment, country ORDER BY is_active_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_segment,
        ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY signup_date DESC, id) row_seq
    FROM valid
    WHERE UPPER(TRIM(name)) = 'CAROL DIAZ'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS rank_in_group
        FROM deduped
    ) x
    WHERE rank_in_group = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles
FROM latest
GROUP BY DATE_TRUNC('MONTH', signup_date)
ORDER BY activity_month DESC
