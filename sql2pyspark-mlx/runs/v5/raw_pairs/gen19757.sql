WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY end_date) AS latest_rank
        FROM subscriptions
    ) t
    WHERE latest_rank = 1
),
base AS (
    SELECT *, LOWER(plan) AS plan_lower
    FROM ranked
    WHERE end_date >= TO_DATE('2023-09-04')
      AND customer_id IS NOT NULL
),
kept_keys AS (
    SELECT customer_id, status
    FROM base
    EXCEPT
    SELECT customer_id, status
    FROM subscriptions
    WHERE MONTH(start_date) IN (1, 7, 8)
),
reconciled AS (
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
matched_keys AS (
    SELECT status, id
    FROM reconciled
    EXCEPT ALL
    SELECT status, id
    FROM subscriptions
    WHERE monthly_fee IS NOT NULL
),
reconciled_subscriptions AS (
    SELECT s.*
    FROM reconciled s
    INNER JOIN matched_keys k ON s.status = k.status AND s.id = k.id
),
joined AS (
    SELECT
        src.customer_id,
        src.end_date,
        src.monthly_fee,
        src.status,
        src.latest_rank,
        b.email,
        b.country,
        b.name
    FROM reconciled_subscriptions src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
joined_subscriptions AS (
    SELECT
        a.*,
        p.credit_limit,
        p.age
    FROM joined a
    LEFT JOIN customers p
        ON a.customer_id = p.id
)
SELECT
    customer_id,
    end_date,
    latest_rank,
    email,
    country,
    credit_limit,
    CASE WHEN country RLIKE '^(JP|US)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS country_zone
FROM joined_subscriptions
ORDER BY customer_id, country_zone;
