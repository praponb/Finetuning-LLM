SELECT
    id,
    supplier_name,
    city,
    LEAD(rating) OVER w_running AS next_rating,
    LAG(rating) OVER w_running AS prev_rating,
    ROW_NUMBER() OVER w_running AS event_seq
FROM suppliers
WHERE city = 'Tokyo'
WINDOW w_running AS (PARTITION BY country, city ORDER BY rating, id)
