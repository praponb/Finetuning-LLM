SELECT signup_date, segment
FROM customers
WHERE city IN ('Tokyo', 'New York') OR age > 59;
