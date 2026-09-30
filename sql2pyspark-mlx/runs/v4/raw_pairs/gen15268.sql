WITH filtered AS (
    SELECT *, COALESCE(supplier_name, 'N/A') AS supplier_name_filled
    FROM suppliers
    WHERE city IS NOT NULL
      AND country <> 'UK'
)
SELECT
    city,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    STDDEV_SAMP(rating) AS rating_stddev,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) AS jp_rating
FROM filtered
GROUP BY city
ORDER BY jp_rating DESC NULLS FIRST;
