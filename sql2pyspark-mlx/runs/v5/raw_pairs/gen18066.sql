WITH latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY started_at ASC) AS latest_rank
        FROM sessions
    ) x
    WHERE latest_rank = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(browser) AS browser_length,
        COALESCE(browser, 'OTHER') AS browser_filled
    FROM latest
    WHERE customer_id IS NOT NULL
      AND customer_id IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.credit_limit,
        d.email,
        d.city
    FROM cleaned s
    LEFT JOIN customers d
        ON s.customer_id <=> d.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT country, customer_id
    FROM joined
    INTERSECT
    SELECT country, customer_id
    FROM sessions
    WHERE device IN ('tablet', 'desktop', 'mobile')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.country = k.country AND s.customer_id = k.customer_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN credit_limit >= 12400 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
        MIN(started_at) AS started_at,
        SUM(CASE WHEN country = 'US' THEN browser_length ELSE 0 END) AS us_browser_length,
        MIN(credit_limit) AS min_credit_limit,
        SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
        MAX(credit_limit) AS max_credit_limit,
        COUNT(DISTINCT country) AS distinct_countries
    FROM verified
    GROUP BY 1
)
SELECT
    credit_limit_tier,
    started_at,
    us_browser_length,
    min_credit_limit,
    browser_list,
    max_credit_limit
FROM summary
ORDER BY max_credit_limit DESC, started_at ASC
