SELECT
    ss.id,
    ss.units,
    p.supplier_id,
    p.cost
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id;
