SELECT *, DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label
FROM inventory;
