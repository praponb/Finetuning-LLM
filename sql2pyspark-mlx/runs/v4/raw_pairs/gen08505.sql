WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        country,
        TRIM(country) AS clean_country,
        TO_DATE(started_at) AS started_at_date,
        COALESCE(browser, 'UNKNOWN') AS browser_clean
    FROM sessions
    WHERE started_at >= DATE '2023-05-16'
      AND customer_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at_date DESC NULLS FIRST, id) AS rank_in_group
        FROM base
    ) ranked
    WHERE rank_in_group = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM latest src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at_date >= sub.start_date
       AND src.started_at_date <= DATE_ADD(sub.end_date, 7)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_window
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY rank_in_group DESC, id, subscription_id) = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(country) AS country_lower,
        COALESCE(country, 'NONE') AS resolved_country
    FROM ranked
    WHERE customer_id IS NOT NULL
      AND monthly_fee BETWEEN 100 AND 200
),
remaining_keys AS (
    -- Set difference on keys
    SELECT browser, id
    FROM valid
    EXCEPT ALL
    SELECT browser, id
    FROM sessions
    WHERE started_at < '2025-06-16'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.browser = k.browser AND s.id = k.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        country,
        clean_country,
        started_at_date,
        rank_in_group,
        plan,
        monthly_fee,
        country_lower,
        resolved_country,
        CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END AS is_enterprise,
        DATE_FORMAT(started_at_date, 'yyyy-MM-dd') AS started_at_date_label
    FROM kept_rows
    WHERE monthly_fee <= 60
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', started_at_date) AS week_start,
        CASE WHEN is_enterprise >= 86 THEN 'HIGH' ELSE 'LOW' END AS is_enterprise_tier,
        COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
        APPROX_PERCENTILE(is_enterprise, 0.5) AS is_enterprise_p50,
        COUNT_IF(country = 'DE') AS de_events,
        PERCENTILE_APPROX(is_enterprise, 0.5) AS is_enterprise_p50_2,
        COUNT_IF(country = 'DE') AS de_events_2
    FROM cleaned
    GROUP BY DATE_TRUNC('WEEK', started_at_date), CASE WHEN is_enterprise >= 86 THEN 'HIGH' ELSE 'LOW' END
    HAVING COUNT(*) FILTER (WHERE plan = 'enterprise') > 1
)
SELECT
    week_start,
    is_enterprise_tier,
    enterprise_rows,
    de_events
FROM totals;
