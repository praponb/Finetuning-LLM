WITH base AS (
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        CAST(started_at AS DATE) AS started_at_day,
        TRIM(country) AS clean_country,
        COALESCE(browser, 'NONE') AS resolved_browser
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND browser IN ('Safari', 'Edge')
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at_day DESC NULLS FIRST, id) AS rn
        FROM base
    ) t
    WHERE rn = 1
),
enriched AS (
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        started_at_day,
        clean_country,
        resolved_browser,
        rn,
        FIRST_VALUE(country, TRUE) OVER (ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        FIRST_VALUE(country, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country_2
    FROM deduped
),
valid AS (
    SELECT
        id,
        device,
        country,
        started_at_day,
        clean_country,
        resolved_browser,
        rn,
        first_known_country,
        first_known_country_2,
        CEIL(rn) AS rn_whole,
        DATEDIFF(CURRENT_DATE(), started_at_day) AS days_since_started_at_day,
        COALESCE(browser, 'DIRECT') AS resolved_browser_2
    FROM enriched
    WHERE started_at >= TO_TIMESTAMP('2023-11-02 22:00:00')
)
SELECT
    id,
    device,
    country,
    started_at_day,
    clean_country,
    rn,
    first_known_country,
    rn_whole,
    days_since_started_at_day
FROM valid
