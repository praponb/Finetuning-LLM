WITH cleaned AS (
    SELECT *, units * revenue revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2024-03-02')
      AND product_id IN (6, 10)
)
SELECT e.*, d.city
FROM cleaned e
LEFT JOIN stores d
    ON e.store_id = d.id
