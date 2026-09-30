WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        INITCAP(TRIM(device)) AS device_title,
        CASE WHEN device = 'tablet' THEN 1 ELSE 0 END AS is_tablet,
        COALESCE(country, 'NONE') AS country_filled
    FROM sessions
    WHERE browser IS DISTINCT FROM 'Chrome'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC NULLS FIRST, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(is_tablet, 2) OVER (PARTITION BY country ORDER BY is_tablet, id) AS next_is_tablet,
        NTILE(4) OVER (ORDER BY is_tablet DESC, id) AS is_tablet_bucket,
        NTILE(5) OVER (PARTITION BY browser ORDER BY is_tablet, id) AS is_tablet_bucket_2,
        MAX(is_tablet) OVER (PARTITION BY country) AS country_max_is_tablet,
        ROW_NUMBER() OVER (ORDER BY rank_in_group DESC, id) AS event_seq
    FROM latest
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        browser,
        device,
        country,
        is_tablet
    FROM islands
) src
PIVOT (
    AVG(is_tablet)
    FOR country IN ('US', 'SG')
)
