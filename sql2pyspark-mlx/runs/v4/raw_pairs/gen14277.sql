WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN country IN ('JP', 'SG') THEN 'Core' ELSE 'Other' END AS country_group
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2024-01-01 06:00:00')
      AND customer_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT country
    FROM cleaned
    EXCEPT
    SELECT country
    FROM sessions
    WHERE HOUR(started_at) BETWEEN 9 AND 17
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.country = k.country
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
),
totals AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', started_at) AS year_start,
        DATE_TRUNC('QUARTER', started_at) AS quarter_start,
        MAX(started_at) AS last_seen,
        (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
        SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
        (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min_2
    FROM eligible
    GROUP BY year_start, quarter_start
    HAVING (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 > 0
)
SELECT
    year_start,
    quarter_start,
    duration_min,
    browser_list,
    duration_min_2
FROM totals
ORDER BY duration_min_2 DESC, quarter_start DESC;
