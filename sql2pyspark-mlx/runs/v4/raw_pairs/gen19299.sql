SELECT cost AS value
FROM shipments
UNION ALL
SELECT amount AS value
FROM payments;
