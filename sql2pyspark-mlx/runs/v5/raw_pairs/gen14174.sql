SELECT shipped_date, carrier
FROM shipments
WHERE carrier NOT IN ('FedEx', 'DHL', 'Flash')
ORDER BY carrier DESC, shipped_date
