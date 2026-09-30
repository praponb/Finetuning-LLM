SELECT *, DATE_TRUNC('month', last_restock) AS last_restock_period FROM inventory;
