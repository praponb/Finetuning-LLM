SELECT *, ROUND(discount * 100, 2) AS adjusted_discount
FROM orders
WHERE (discount < 0.26 AND shipping_fee BETWEEN 15 AND 17) OR channel <> 'web'
