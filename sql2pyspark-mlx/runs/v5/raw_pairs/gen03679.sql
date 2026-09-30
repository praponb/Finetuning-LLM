WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('/', device, country) AS device_country_key,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM sessions
    WHERE browser IN ('Edge', 'Safari')
      AND country IS DISTINCT FROM 'DE'
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at >= sub.start_date
       AND src.started_at <= DATE_ADD(sub.end_date, 10)
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.country,
        s.device_country_key,
        s.country_clean,
        s.subscription_id,
        s.plan,
        c.email,
        c.segment
    FROM with_window s
    LEFT JOIN customers c
        ON s.customer_id <=> c.id
    WHERE YEAR(c.signup_date) >= 2023
),
kept_keys AS (
    -- Set intersection on keys
    SELECT device, country
    FROM joined
    INTERSECT
    SELECT device, country
    FROM sessions
    WHERE started_at < '2024-10-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.device = k.device AND s.country = k.country
)
-- Join a dimension and aggregate
SELECT
    d.segment AS customer_segment,
    src.device,
    COUNT(*) AS num_rows,
    MIN(src.subscription_id) AS min_subscription_id
FROM reconciled src
LEFT JOIN customers d ON src.customer_id = d.id
GROUP BY GROUPING SETS ((d.segment, src.device), (d.segment), ());
