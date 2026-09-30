WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(browser)) AS browser_title
    FROM sessions
    WHERE started_at > TO_DATE('2023-07-21')
      AND browser IS NOT NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT browser, customer_id
    FROM valid
    INTERSECT ALL
    SELECT browser, customer_id
    FROM sessions
    WHERE browser <> 'Safari'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.browser = k.browser AND s.customer_id = k.customer_id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.duration_sec BETWEEN 348 AND 370
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.age,
        d.name
    FROM kept s
    INNER JOIN customers d
        ON s.customer_id = d.id
),
valid_sessions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(country, 'N/A') AS country_filled,
        CASE WHEN browser IN ('Chrome', 'Safari') THEN 'Core' ELSE 'Other' END AS browser_group,
        COALESCE(country, 'DIRECT') AS country_filled_2
    FROM with_dims
    WHERE country IS NOT NULL
),
with_dims_sessions AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.name AS customer_name,
        b.country AS customer_country
    FROM valid_sessions s
    LEFT JOIN customers b
        ON s.customer_id <=> b.id
)
SELECT
    id,
    device,
    browser,
    started_at,
    country,
    browser_title,
    country_filled,
    browser_group,
    country_filled_2
FROM with_dims_sessions
ORDER BY id DESC NULLS LAST;
