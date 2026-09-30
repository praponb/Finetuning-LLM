WITH cleaned AS (
    SELECT
        *,
        CAST(age AS DOUBLE) AS age_str,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM customers
    WHERE signup_date >= DATE '2023-06-02'
),
enriched AS (
    SELECT
        *,
        MIN(age) OVER w_ordered AS running_min_age,
        MAX(age) OVER w_ordered AS running_max_age,
        SUM(age) OVER w_ordered AS running_sum_age,
        COUNT(age) OVER w_ordered AS running_count_age,
        LAST_VALUE(city, TRUE) OVER (PARTITION BY country ORDER BY age_str ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY city ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_customers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id ASC) AS row_seq,
        ROUND(running_min_age / SUM(running_min_age) OVER (PARTITION BY segment), 4) AS running_min_age_share
    FROM enriched
),
enriched_customers_2 AS (
    SELECT
        *,
        FIRST_VALUE(running_max_age) IGNORE NULLS OVER w_hist AS first_known_running_max_age,
        AVG(running_min_age_share) OVER (PARTITION BY country ORDER BY running_min_age_share ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_min_age_share_avg_3,
        AVG(age_str) OVER (ORDER BY age_str ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS age_str_avg_5,
        SUM(running_count_age) OVER (PARTITION BY city) AS city_sum_running_count_age,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM enriched_customers
    WHERE running_min_age_share > 0.34
    WINDOW w_hist AS (PARTITION BY segment ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY country ORDER BY running_count_age ASC, id ASC)
),
summary AS (
    SELECT
        DATE_TRUNC('WEEK', signup_date) AS week_start,
        COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
        BIT_OR(age) AS age_bit_or,
        MAX(seq_no) FILTER (WHERE city = 'London') AS london_max_seq_no,
        PERCENTILE_APPROX(age_str_avg_5, ARRAY(0.25, 0.5, 0.75)) AS age_str_avg_5_quartiles,
        COUNT(*) AS cnt,
        ROUND(AVG(seq_no), 2) AS avg_seq_no
    FROM enriched_customers_2
    GROUP BY DATE_TRUNC('WEEK', signup_date)
)
SELECT
    week_start,
    sg_count,
    age_bit_or,
    age_str_avg_5_quartiles
FROM summary;
