WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        COALESCE(country, 'Unknown') AS country_filled
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-05-27 01:00:00'
      AND browser IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.started_at,
        b.credit_limit,
        b.signup_date,
        b.name
    FROM cleaned a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_dims
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM islands
    EXCEPT ALL
    SELECT customer_id, id
    FROM sessions
    WHERE customer_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
with_dims_sessions AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.credit_limit,
        a.signup_date,
        a.name,
        a.new_group_flag,
        a.island_id,
        c.segment,
        c.city
    FROM verified a
    INNER JOIN customers c
        ON a.customer_id = c.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.city AS customer_city,
        p.age,
        p.email
    FROM with_dims_sessions e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
    WHERE p.segment = 'Corporate'
),
cleaned_sessions AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(name) AS name_lower
    FROM enriched
    WHERE MONTH(signup_date) IN (2, 9, 12)
)
-- Join a dimension and aggregate
SELECT
    d.city AS customer_city,
    a.customer_city AS session_customer_city,
    COUNT(*) AS rows_total,
    MIN(a.island_id) AS min_island_id,
    GROUPING_ID() AS gid
FROM cleaned_sessions a
INNER JOIN customers d ON a.customer_id = d.id
GROUP BY GROUPING SETS ((d.city, a.customer_city), (d.city), ());
