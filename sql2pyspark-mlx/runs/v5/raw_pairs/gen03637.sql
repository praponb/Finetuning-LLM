WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY signup_date, id) AS dedup_rank
    FROM customers
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    name,
    email,
    country,
    city,
    signup_date,
    is_active,
    dedup_rank,
    CASE WHEN country RLIKE '^(TH|UK)$' THEN 'OTHER_ZONE' WHEN country RLIKE '^(US|SG)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS country_zone
FROM deduped
ORDER BY dedup_rank DESC NULLS LAST;
