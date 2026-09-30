SELECT channel, status FROM orders INTERSECT SELECT channel, status FROM orders WHERE amount >= 730;
