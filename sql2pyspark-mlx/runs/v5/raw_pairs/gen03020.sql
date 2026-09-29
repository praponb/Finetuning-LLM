WITH lines AS (
    SELECT
        id,
        customer_id,
        item.product_id,
        item.price
    FROM (
        SELECT
            id,
            customer_id,
            EXPLODE_OUTER(items) AS item
        FROM carts
    ) t
),
valid AS (
    SELECT *, COALESCE(product_id, 0) AS product_id_filled
    FROM lines
    WHERE customer_id IN (8, 21, 30, 38)
),
joined AS (
    SELECT
        src.*,
        b.city,
        b.country
    FROM valid src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
with_dims AS (
    SELECT
        src.id,
        src.customer_id,
        src.price,
        src.product_id_filled,
        src.country,
        p.signup_date
    FROM joined src
    LEFT JOIN customers p
        ON src.customer_id <=> p.id
)
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    customer_id,
    COUNT_IF(country = 'UK') AS uk_events,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', signup_date) ORDER BY COUNT(CASE WHEN country = 'UK' THEN 1 END) DESC) AS uk_count_dense_rank
FROM with_dims
GROUP BY DATE_TRUNC('QUARTER', signup_date), customer_id
HAVING COUNT_IF(country = 'UK') > 1
ORDER BY uk_events DESC
