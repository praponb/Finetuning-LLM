WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, country
    FROM islands
    EXCEPT DISTINCT
    SELECT id, country
    FROM sessions
    WHERE browser = 'Edge'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN kept_keys k ON s.id = k.id AND s.country = k.country
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(browser)) AS browser_title,
        UPPER(device) AS device_upper
    FROM kept_rows
    WHERE started_at >= DATE '2023-04-09'
      AND browser IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        session_no,
        browser_title,
        device_upper,
        CASE WHEN country = 'TH' THEN 1 ELSE 0 END AS is_th,
        COALESCE(browser, 'DIRECT') AS browser_filled
    FROM filtered
    WHERE started_at > DATE '2023-03-26'
      AND country IS NOT NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT browser, id
    FROM base
    INTERSECT ALL
    SELECT browser, id
    FROM sessions
    WHERE device IN ('mobile', 'desktop', 'tablet')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.browser = k.browser AND s.id = k.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.segment,
        d.name,
        d.email
    FROM verified src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.device,
        src.browser,
        src.started_at,
        src.browser_title,
        src.is_th,
        src.segment,
        src.email,
        d.signup_date,
        d.email AS customer_email,
        d.age
    FROM with_dims src
    LEFT JOIN customers d
        ON src.customer_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    segment,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    GROUPING(customer_id) AS grouping_level
FROM enriched
GROUP BY GROUPING SETS ((customer_id, DATE_TRUNC('MONTH', started_at), segment), (customer_id, DATE_TRUNC('MONTH', started_at)), ())
HAVING num_browsers >= 1
ORDER BY activity_month DESC;
