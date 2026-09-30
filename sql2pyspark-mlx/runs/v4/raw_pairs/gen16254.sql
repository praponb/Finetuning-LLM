SELECT shipped_date, 'shipment' AS source
FROM shipments
UNION
SELECT sale_date AS shipped_date, 'store_sale' AS source
FROM store_sales
