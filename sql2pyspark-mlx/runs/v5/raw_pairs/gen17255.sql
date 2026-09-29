SELECT last_restock FROM inventory WHERE YEAR(last_restock) >= 2023 INTERSECT SELECT opened_date FROM stores;
