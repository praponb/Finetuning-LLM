SELECT method FROM payments WHERE method IN ('bank_transfer', 'cash') UNION SELECT method FROM payments WHERE amount < 690 ORDER BY method DESC;
