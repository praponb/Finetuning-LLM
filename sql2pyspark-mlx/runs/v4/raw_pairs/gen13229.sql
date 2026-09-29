WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(city) IGNORE NULLS OVER (ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        SUM(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) AS seq_no,
        ROUND(age / SUM(age) OVER (PARTITION BY country, city), 4) AS age_share,
        LEAD(credit_limit, 2) OVER w_hist AS next_credit_limit
    FROM customers
    WINDOW w_hist AS (ORDER BY signup_date, id)
),
metrics_customers AS (
    -- Window metrics over several different partitions
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
        first_known_city,
        cumulative_age,
        seq_no,
        age_share,
        next_credit_limit,
        AVG(seq_no) OVER (PARTITION BY country) AS country_avg_seq_no,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY country), 4) AS seq_no_share
    FROM metrics
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(age) OVER w_recent AS cumulative_age_2,
        PERCENT_RANK() OVER w_ordered AS country_avg_seq_no_percent_rank
    FROM metrics_customers
    WINDOW w_recent AS (PARTITION BY country ORDER BY seq_no_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY city, segment ORDER BY country_avg_seq_no DESC)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        segment,
        city,
        age_share
    FROM enriched
) src
PIVOT (
    MAX(age_share)
    FOR city IN ('Singapore', 'Bangkok')
);
