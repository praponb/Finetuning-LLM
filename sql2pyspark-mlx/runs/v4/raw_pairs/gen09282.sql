WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        CONCAT_WS('-', country, browser) AS country_browser_key,
        COALESCE(browser, 'N/A') AS browser_clean
    FROM sessions
    WHERE country IS NOT NULL
)
-- Ranking and gap features
SELECT
    *,
    LAST_VALUE(browser, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
    ROW_NUMBER() OVER (ORDER BY started_at DESC, id) AS row_seq,
    LAST_VALUE(country, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country
FROM valid
