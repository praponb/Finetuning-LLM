SELECT amount FROM orders WHERE discount <= 0.24 OR channel IN ('store', 'mobile') ORDER BY amount;
