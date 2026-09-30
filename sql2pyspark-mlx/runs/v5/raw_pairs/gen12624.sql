SELECT
    id,
    city,
    rating,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) row_num,
    SUM(rating) OVER (PARTITION BY city) city_sum_rating
FROM suppliers
ORDER BY row_num DESC;
