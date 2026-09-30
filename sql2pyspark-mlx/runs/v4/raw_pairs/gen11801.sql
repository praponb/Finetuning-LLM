WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date >= s.signup_date
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY age DESC) AS latest_rank
        FROM not_flagged
    ) ranked
    WHERE latest_rank = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.account_type IN ('LOAN', 'SAVINGS')
        )
),
not_flagged_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date >= s.signup_date
        )
)
SELECT
    id,
    name,
    email,
    country,
    segment,
    age,
    latest_rank,
    CASE WHEN name RLIKE '^(Emma Brown|Henry Ford)$' THEN 'CORE' ELSE 'GROUP_A' END AS name_zone
FROM not_flagged_customers
ORDER BY latest_rank DESC NULLS LAST
