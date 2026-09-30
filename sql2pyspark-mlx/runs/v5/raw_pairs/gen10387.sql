SELECT id, rating, RANK() OVER (ORDER BY rating DESC) AS rnk FROM suppliers ORDER BY rating DESC, rnk;
