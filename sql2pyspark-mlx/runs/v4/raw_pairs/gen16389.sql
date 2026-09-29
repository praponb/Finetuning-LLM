WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rn
    FROM order_items
    WHERE unit_price < 280
)
SELECT *
FROM numbered
WHERE rn <= 3
ORDER BY rn DESC;
