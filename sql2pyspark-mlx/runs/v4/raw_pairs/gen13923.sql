WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(city) IGNORE NULLS OVER w_running AS last_known_city,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY city), 4) AS credit_limit_share
    FROM customers
    WINDOW w_running AS (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date BETWEEN '2025-01-01' AND '2025-09-30'
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date <= s.signup_date
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM not_flagged
    QUALIFY RANK() OVER (PARTITION BY city, segment ORDER BY credit_limit_share DESC NULLS LAST) = 1
)
SELECT
    id,
    email,
    country,
    city,
    segment,
    signup_date,
    credit_limit,
    age,
    last_known_city,
    CASE WHEN country RLIKE '^(UK|US)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS country_zone
FROM ranked
ORDER BY country_zone
