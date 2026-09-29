WITH valid_suppliers AS (
    SELECT *, CASE WHEN country IN ('UK', 'US') THEN 'Core' ELSE 'Other' END AS country_group
    FROM suppliers
    WHERE country NOT IN ('JP', 'US', 'UK')
),
RatingBucketSummary AS (
    SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN country = 'SG' THEN rating ELSE 0 END) AS sg_rating, COUNT(DISTINCT rating) AS unique_countrys
    FROM valid_suppliers
    GROUP BY CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END
)
SELECT rating_bucket, sg_rating
FROM RatingBucketSummary
WHERE sg_rating < 27
