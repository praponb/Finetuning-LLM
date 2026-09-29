WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        SUBSTRING(device, 1, 2) AS device_prefix,
        UPPER(device) AS device_upper
    FROM sessions
    WHERE HOUR(started_at) BETWEEN 12 AND 20
      AND customer_id IS NOT NULL
),
cleaned_sessions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        started_at,
        country,
        device_prefix,
        device_upper,
        YEAR(started_at) AS started_at_year
    FROM cleaned
    WHERE started_at >= DATE '2023-02-27'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(started_at_year) AS started_at_year_whole
    FROM cleaned_sessions
    WHERE customer_id IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, country
    FROM filtered
    EXCEPT
    SELECT customer_id, country
    FROM sessions
    WHERE country = 'SG'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.country = k.country
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.started_at
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, country
    FROM not_flagged
    EXCEPT DISTINCT
    SELECT customer_id, country
    FROM sessions
    WHERE country IN ('DE', 'UK', 'TH')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.country = k.country
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.started_at
        )
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', started_at) AS quarter_start,
        COUNT(*) FILTER (WHERE country = 'DE') AS de_rows,
        COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
        MAX(started_at_year) FILTER (WHERE country = 'JP') AS jp_max_started_at_year,
        MAX(started_at) AS ended_at
    FROM kept
    GROUP BY ROLLUP (DATE_TRUNC('QUARTER', started_at))
)
SELECT
    de_rows,
    sg_count,
    ended_at
FROM stats
ORDER BY de_rows
