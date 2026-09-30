SELECT id, amount, order_id, TO_DATE(paid_at) paid_at_date FROM payments WHERE method IN ('bank_transfer', 'paypal', 'cash') AND status IN ('FAILED', 'REFUNDED', 'SUCCESS') AND amount < 540;
