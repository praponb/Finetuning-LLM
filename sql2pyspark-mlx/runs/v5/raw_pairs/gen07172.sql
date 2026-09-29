SELECT
    category,
    brand_group,
    max_price,
    brand_values
FROM (
    SELECT
        category,
        CASE WHEN brand IN ('Globex', 'Initech') THEN 'group_a' ELSE 'other' END AS brand_group,
        MAX(price) AS max_price,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values
    FROM products
    GROUP BY category, CASE WHEN brand IN ('Globex', 'Initech') THEN 'group_a' ELSE 'other' END
) AS x
WHERE max_price < 1080
ORDER BY category ASC;
