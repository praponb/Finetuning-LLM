WITH supplier_totals AS (
    SELECT supplier_id, MAX(cost) AS max_cost
    FROM products
    GROUP BY supplier_id
)
SELECT suppliers.country, supplier_totals.max_cost
FROM suppliers
INNER JOIN supplier_totals ON suppliers.id = supplier_totals.supplier_id;
