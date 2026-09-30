WITH valid AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        COALESCE(age, 0) AS age_filled,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
    FROM customers
    WHERE signup_date > TO_DATE('2023-09-07')
),
with_windows AS (
    SELECT
        *,
        ROUND(age / SUM(age) OVER (PARTITION BY city, segment), 4) AS age_share,
        COUNT(age_filled) OVER (PARTITION BY country, segment) AS country_count_age_filled,
        SUM(age) OVER (PARTITION BY segment ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM valid
    WINDOW w_recent AS (PARTITION BY segment, city ORDER BY signup_date DESC, id ASC)
),
with_windows_customers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY country_count_age_filled ASC, id ASC) AS seq_no,
        NTILE(4) OVER (ORDER BY age_share DESC, id ASC) AS age_share_bucket,
        SUM(age) OVER (ORDER BY cumulative_age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age
    FROM with_windows
)
SELECT
    seq_no,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    MAX(cumulative_age) FILTER (WHERE city = 'Tokyo') AS tokyo_max_cumulative_age,
    APPROX_PERCENTILE(running_age, ARRAY(0.25, 0.5, 0.75)) AS running_age_quartiles,
    BIT_AND(age) AS age_bit_and
FROM with_windows_customers
GROUP BY seq_no
ORDER BY age_bit_and ASC NULLS FIRST;
