SELECT id, delivered_date, UPPER(status) AS status_upper
FROM shipments
WHERE carrier != 'DHL' AND cost BETWEEN 28 AND 55
