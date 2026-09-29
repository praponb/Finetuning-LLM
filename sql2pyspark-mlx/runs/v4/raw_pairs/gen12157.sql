WITH with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS event_seq,
        LAG(rating, 1) OVER (ORDER BY rating, id) AS prev_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rating_avg_6
    FROM suppliers
)
SELECT
    CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    SIZE(COLLECT_SET(city)) AS num_cities,
    PERCENTILE_APPROX(running_rating, 0.9) AS running_rating_p90
FROM with_windows
GROUP BY event_seq_tier;
