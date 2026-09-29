WITH with_windows AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        ROW_NUMBER() OVER (ORDER BY started_at, id) seq_no,
        FIRST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_browser,
        FIRST_VALUE(browser) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_browser_2
    FROM sessions
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, country ORDER BY seq_no DESC NULLS LAST, id) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
),
cleaned AS (
    SELECT
        id,
        customer_id,
        started_at,
        country,
        seq_no,
        first_known_browser,
        first_known_browser_2,
        SUBSTRING(browser, 1, 4) browser_prefix,
        COALESCE(country, 'UNKNOWN') resolved_country
    FROM latest
    WHERE seq_no BETWEEN 4 AND 8
)
SELECT
    customer_id,
    CASE WHEN seq_no >= 6 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    seq_no,
    MAX(seq_no) FILTER (WHERE country = 'UK') AS uk_max_seq_no,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    SUM(CASE WHEN country = 'SG' THEN seq_no ELSE 0 END) AS sg_seq_no,
    COUNT_IF(country = 'US') AS us_events,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles_2
FROM cleaned
GROUP BY customer_id, CASE WHEN seq_no >= 6 THEN 'LARGE' ELSE 'SMALL' END, seq_no;
