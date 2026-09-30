SELECT shipped_date, 'shipment' AS source
FROM shipments
UNION ALL
SELECT opened_date, 'account' AS source
FROM accounts
WHERE opened_date BETWEEN '2023-06-01' AND '2023-08-27'
ORDER BY shipped_date;
