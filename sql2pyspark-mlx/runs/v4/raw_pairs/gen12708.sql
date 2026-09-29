WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN country IN ('UK', 'TH') THEN 'Tier 1' ELSE 'Other' END AS country_group,
        REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain,
        COALESCE(segment, 'DIRECT') AS segment_filled
    FROM customers
    WHERE signup_date > DATE '2023-07-16'
      AND city IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        country,
        city,
        age
    FROM cleaned
) src
PIVOT (
    MAX(age)
    FOR city IN ('New York' AS new_york, 'Berlin' AS berlin, 'Tokyo' AS tokyo)
)
