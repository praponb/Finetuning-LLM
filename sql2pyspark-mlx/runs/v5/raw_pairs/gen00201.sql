SELECT delivered_date, 'shipment' AS origin FROM shipments WHERE cost BETWEEN 55 AND 58 UNION SELECT opened_date, 'store' AS origin FROM stores WHERE opened_date >= '2023-04-03'
