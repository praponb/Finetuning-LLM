WITH valid AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        DATE_TRUNC('year', started_at) AS started_at_period,
        COALESCE(browser, 'OTHER') AS resolved_browser
    FROM sessions
    WHERE customer_id IS NOT NULL
),
remaining_keys AS (
    SELECT customer_id, country
    FROM valid
    INTERSECT ALL
    SELECT customer_id, country
    FROM sessions
    WHERE TO_DATE(started_at) < '2025-08-01'
),
verified AS (
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.country = k.country
),
joined AS (
    SELECT
        a.*,
        d.email,
        d.name,
        d.age
    FROM verified a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
with_windows AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        started_at_period,
        resolved_browser,
        email,
        name,
        age,
        STDDEV_SAMP(age) OVER (PARTITION BY browser) AS browser_stddev_samp_age,
        AVG(age) OVER w_recent AS age_avg_30d
    FROM joined
    WINDOW w_recent AS (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
)
SELECT *
FROM (
    SELECT
        customer_id,
        browser,
        browser_stddev_samp_age
    FROM with_windows
) src
PIVOT (
    SUM(browser_stddev_samp_age) AS value_sum,
    COUNT(1) AS item_count
    FOR browser IN ('Chrome' AS chrome, 'Firefox' AS firefox)
);
