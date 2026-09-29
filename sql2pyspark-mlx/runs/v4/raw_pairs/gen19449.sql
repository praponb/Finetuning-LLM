WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        COALESCE(city, 'N/A') AS city_filled,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM customers
    WHERE signup_date > TO_DATE('2023-09-18')
      AND age BETWEEN 40 AND 49
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, country
    FROM filtered
    EXCEPT
    SELECT id, country
    FROM customers
    WHERE credit_limit BETWEEN 9500 AND 11900
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.country = k.country
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        city_filled,
        city_clean,
        CORR(credit_limit, age) OVER w_hist AS corr_credit_limit_age,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit NULLS FIRST) AS credit_limit_dense_rank,
        MAX(age) OVER w_hist AS city_max_age,
        ROUND(age / SUM(age) OVER (PARTITION BY country, segment), 4) AS age_share,
        ROUND(age / SUM(age) OVER (PARTITION BY segment, city), 4) AS age_share_2
    FROM verified
    WINDOW w_hist AS (PARTITION BY city)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(age_share_2) AS age_share_2_whole,
        CASE WHEN country IN ('JP', 'TH') THEN 'Priority' ELSE 'Other' END AS country_group,
        COALESCE(segment, 'DIRECT') AS resolved_segment
    FROM with_windows
    WHERE credit_limit IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.customer_id IS NOT NULL
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.device = 'desktop'
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN age >= 63 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    CASE WHEN corr_credit_limit_age >= 0.65 THEN 'LARGE' ELSE 'SMALL' END AS corr_credit_limit_age_tier,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    ROUND(AVG(corr_credit_limit_age), 2) AS avg_corr_credit_limit_age,
    ROUND(SUM(corr_credit_limit_age) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS corr_credit_limit_age_per_tokyo
FROM kept
GROUP BY GROUPING SETS ((CASE WHEN age >= 63 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN corr_credit_limit_age >= 0.65 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN age >= 63 THEN 'LARGE' ELSE 'SMALL' END))
HAVING corr_credit_limit_age_per_tokyo < 0
ORDER BY age_tier DESC NULLS LAST;
