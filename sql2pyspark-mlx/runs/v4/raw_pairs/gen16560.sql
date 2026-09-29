SELECT DISTINCT status FROM payments WHERE method IN ('cash', 'card', 'bank_transfer') ORDER BY status;
