WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(browser) AS browser_upper,
        CONCAT(browser, ' ', device) AS browser_device
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-06-07 22:00:00'
      AND browser IS DISTINCT FROM 'Firefox'
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.device,
        src.browser,
        src.started_at,
        src.browser_device,
        p.email,
        p.credit_limit,
        p.country AS customer_country
    FROM filtered src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM joined
    EXCEPT
    SELECT customer_id
    FROM sessions
    WHERE customer_id IN (10, 18)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN matched_keys k ON s.customer_id = k.customer_id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.credit_limit AS customer_credit_limit,
        c.signup_date
    FROM verified src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.duration_sec < 177
        )
),
joined_sessions AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.signup_date AS customer_signup_date,
        b.country
    FROM not_flagged e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.browser,
        e.browser_device,
        e.email,
        e.signup_date,
        e.customer_signup_date,
        e.country,
        b.country AS customer_country_2,
        b.segment
    FROM joined_sessions e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    segment,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(DISTINCT segment) AS distinct_segments,
    SORT_ARRAY(COLLECT_SET(customer_country_2)) AS customer_country_2_list,
    COUNT(CASE WHEN customer_country_2 = 'JP' THEN 1 END) AS jp_count
FROM with_dims
GROUP BY CUBE (segment);
