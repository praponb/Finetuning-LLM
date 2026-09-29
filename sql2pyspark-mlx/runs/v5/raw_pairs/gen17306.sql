WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(country) clean_country,
        COALESCE(browser, 'NONE') browser_filled
    FROM sessions
    WHERE country IS NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.email,
        b.credit_limit
    FROM filtered e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT browser
    FROM joined
    INTERSECT ALL
    SELECT browser
    FROM sessions
    WHERE country IN ('SG', 'US')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN remaining_keys k ON s.browser = k.browser
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        browser,
        country,
        clean_country,
        browser_filled,
        email,
        ROUND(credit_limit * 1.07, 2) credit_limit_scaled,
        COALESCE(country, 'UNKNOWN') country_clean
    FROM kept_rows
    WHERE started_at > TO_DATE('2023-02-15')
      AND browser IS DISTINCT FROM 'Chrome'
),
base_sessions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        customer_id,
        country,
        clean_country,
        browser_filled,
        email,
        credit_limit_scaled,
        country_clean,
        LOWER(country) country_lower,
        COALESCE(browser, 'UNKNOWN') browser_clean
    FROM base
    WHERE country IS NOT NULL
),
filtered_sessions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS(',', email, country) email_country_key,
        COALESCE(country, 'NONE') country_filled
    FROM base_sessions
    WHERE customer_id IS NOT NULL
      AND country IS DISTINCT FROM 'JP'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        customer_id,
        browser_filled,
        country_clean,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY credit_limit_scaled DESC NULLS LAST) credit_limit_scaled_percent_rank,
        STDDEV_SAMP(credit_limit_scaled) OVER (PARTITION BY customer_id) customer_id_stddev_samp_credit_limit_scaled
    FROM filtered_sessions
)
-- Join a dimension and aggregate
SELECT
    dim.segment,
    dim.country,
    COUNT(*) AS rows_total,
    MIN(src.customer_id_stddev_samp_credit_limit_scaled) AS min_customer_id_stddev_samp_credit_limit_scaled
FROM enriched src
JOIN customers dim ON src.customer_id = dim.id
GROUP BY ROLLUP (dim.segment, dim.country);
