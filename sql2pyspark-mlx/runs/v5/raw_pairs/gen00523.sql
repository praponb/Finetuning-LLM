WITH metrics AS (
    SELECT
        id,
        supplier_name,
        city,
        LAG(rating) OVER w_part AS prev_rating,
        LEAD(rating) OVER w_part AS next_rating,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM suppliers
    WINDOW w_part AS (PARTITION BY city ORDER BY rating, id)
)
SELECT
    id,
    supplier_name,
    city,
    prev_rating,
    next_rating,
    event_seq,
    CASE WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'PRIORITY' WHEN city RLIKE '^(New York|Singapore)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS city_zone
FROM metrics;
