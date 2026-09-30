SELECT *, LAG(amount) OVER w_hist prev_amount, ROW_NUMBER() OVER w_hist event_seq FROM payments WHERE HOUR(paid_at) BETWEEN 9 AND 15 WINDOW w_hist AS (PARTITION BY method ORDER BY amount, id)
