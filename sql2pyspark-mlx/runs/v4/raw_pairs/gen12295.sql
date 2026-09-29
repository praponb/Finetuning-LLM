SELECT store_id, SUM(amount) total_amount, SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) pending_count
FROM orders
WHERE discount <= 0.26 AND amount > 1190 AND shipping_fee BETWEEN 14 AND 25
GROUP BY store_id
ORDER BY pending_count DESC;
