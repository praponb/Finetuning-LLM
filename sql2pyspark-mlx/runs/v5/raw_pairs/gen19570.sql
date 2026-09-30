SELECT
    id,
    city,
    region,
    opened_date,
    LEAD(sqft) OVER (PARTITION BY region, city ORDER BY opened_date, id) AS next_sqft,
    ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date, id) AS row_seq
FROM stores
WHERE sqft < 5246;
