SELECT last_restock FROM inventory WHERE (quantity BETWEEN 125 AND 239 AND quantity < 130) OR store_id IN (1, 3, 5) ORDER BY last_restock DESC;
