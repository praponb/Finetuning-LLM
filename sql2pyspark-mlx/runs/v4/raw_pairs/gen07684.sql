SELECT delivered_date, status
FROM shipments
WHERE carrier != 'Kerry' AND status = 'DELIVERED'
