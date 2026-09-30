SELECT status, INITCAP(TRIM(status)) AS status_title FROM payments WHERE method = 'paypal' AND (amount <= 1480 OR TO_DATE(paid_at) >= '2023-11-24');
