WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_ordered AS credit_limit_cume_dist,
        FIRST_VALUE(age) IGNORE NULLS OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age
    FROM customers
    WINDOW w_ordered AS (PARTITION BY city ORDER BY credit_limit NULLS LAST)
)
-- Grouped metrics
SELECT
    city,
    BIT_AND(age) AS age_bit_and,
    AVG(credit_limit) FILTER (WHERE city = 'Singapore') AS singapore_avg_credit_limit,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    ROUND(AVG(credit_limit_cume_dist), 2) AS avg_credit_limit_cume_dist
FROM with_windows
GROUP BY city
HAVING avg_credit_limit_cume_dist >= 0;
