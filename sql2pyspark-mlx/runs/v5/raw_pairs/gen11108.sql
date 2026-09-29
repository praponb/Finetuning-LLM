SELECT carrier, MAX(shipped_date) AS last_shipped_date
FROM shipments
WHERE delivered_date <= '2025-02-01' AND shipped_date > '2024-04-01' AND carrier = 'FedEx'
GROUP BY carrier;
