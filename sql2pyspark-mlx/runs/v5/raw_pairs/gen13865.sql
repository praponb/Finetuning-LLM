SELECT *, CASE WHEN cost >= 95 THEN 'High' WHEN cost >= 42 THEN 'Medium' ELSE 'Low' END cost_band, CONCAT(status, ' - ', carrier) status_carrier FROM shipments ORDER BY id;
