SELECT
    id,
    segment,
    age,
    MAX(age) OVER (PARTITION BY segment) AS segment_max_age
FROM customers
WHERE age >= 46
ORDER BY id;
