WITH metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_recent seq_no,
        FIRST_VALUE(browser, TRUE) OVER w_hist first_known_browser
    FROM sessions
    WINDOW w_recent AS (PARTITION BY device, country ORDER BY started_at, id), w_hist AS (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT
        *,
        LENGTH(country) country_length,
        ROUND(seq_no, 0) seq_no_rounded
    FROM metrics
    WHERE started_at > DATE '2023-03-28'
      AND customer_id IS NOT NULL
),
joined AS (
    SELECT
        s.*,
        b.credit_limit,
        b.age
    FROM filtered s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
enriched AS (
    SELECT
        *,
        AVG(seq_no_rounded) OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) seq_no_rounded_avg_3,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY seq_no, id) seq_no_bucket,
        LAST_VALUE(country_length) IGNORE NULLS OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_country_length,
        SUM(credit_limit) OVER (PARTITION BY customer_id ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) credit_limit_sum_7d
    FROM joined
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
joined_sessions AS (
    SELECT
        e.*,
        c.age AS customer_age,
        c.segment
    FROM sessionized e
    JOIN customers c
        ON e.customer_id = c.id
)
SELECT *
FROM (
    SELECT
        segment,
        browser,
        device,
        age
    FROM joined_sessions
) src
PIVOT (
    SUM(age)
    FOR device IN ('desktop' AS desktop, 'mobile' AS mobile)
);
