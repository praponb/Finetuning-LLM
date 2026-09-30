SELECT *, CASE WHEN carrier IN ('Kerry', 'FedEx') THEN 'Group A' ELSE 'Other' END AS carrier_group FROM shipments WHERE MONTH(shipped_date) IN (6, 7, 9) ORDER BY carrier_group DESC
