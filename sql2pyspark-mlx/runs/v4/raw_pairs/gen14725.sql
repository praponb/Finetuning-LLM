SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft DESC NULLS LAST, id) AS rank_in_group
    FROM stores
) ranked
WHERE rank_in_group = 1
