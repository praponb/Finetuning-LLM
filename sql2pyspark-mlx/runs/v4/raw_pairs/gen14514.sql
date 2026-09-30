SELECT sale_date, 'store_sale' AS source FROM store_sales UNION ALL SELECT delivered_date, 'shipment' AS source FROM shipments WHERE shipped_date BETWEEN '2024-12-01' AND '2025-08-01'
