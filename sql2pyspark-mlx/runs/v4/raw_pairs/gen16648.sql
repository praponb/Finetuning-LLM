SELECT method, order_id FROM payments EXCEPT ALL SELECT method, order_id FROM payments WHERE method IN ('bank_transfer', 'promptpay');
