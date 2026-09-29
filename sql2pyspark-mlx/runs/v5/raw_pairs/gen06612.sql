SELECT channel, status, SUM(CASE WHEN channel = 'mobile' THEN amount ELSE 0 END) mobile_amount
FROM orders
WHERE amount > 590 AND discount < 0.13
GROUP BY channel, status
ORDER BY mobile_amount;
