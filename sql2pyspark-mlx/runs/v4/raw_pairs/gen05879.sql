WITH scored AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY country, segment ORDER BY credit_limit DESC, id ASC) credit_limit_bucket,
        NTILE(3) OVER (PARTITION BY country ORDER BY age ASC, id ASC) age_bucket,
        AVG(credit_limit) OVER w_running credit_limit_avg_6_prior
    FROM customers
    WINDOW w_running AS (PARTITION BY country, segment ORDER BY credit_limit ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
SELECT
    segment,
    CASE WHEN age_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS age_bucket_tier,
    CASE WHEN credit_limit_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_bucket_tier,
    SIZE(COLLECT_SET(city)) AS num_cities,
    STDDEV_SAMP(age) AS age_stddev,
    APPROX_PERCENTILE(credit_limit, 0.95) AS credit_limit_p95,
    COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count,
    MAX(age) AS max_age
FROM scored
GROUP BY 1, 2, 3;
