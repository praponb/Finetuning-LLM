WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ADD_MONTHS(signup_date, 3) AS signup_date_plus_months,
        CASE WHEN credit_limit >= 14500 THEN 'High' WHEN credit_limit >= 5300 THEN 'Medium' ELSE 'Low' END AS credit_limit_level
    FROM customers
    WHERE signup_date > TO_DATE('2023-03-27')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.end_date >= s.signup_date_plus_months
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS latest_rank
        FROM not_flagged
    ) t
    WHERE latest_rank = 1
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN credit_limit >= 13500 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
        DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
        COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        ROUND(AVG(age), 2) AS avg_age
    FROM latest
    GROUP BY CUBE (CASE WHEN credit_limit >= 13500 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', signup_date))
)
SELECT chiang_mai_count, city_list
FROM totals
ORDER BY chiang_mai_count DESC NULLS LAST;
