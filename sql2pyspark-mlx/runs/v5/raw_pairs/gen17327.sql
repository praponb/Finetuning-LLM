select id, last_restock from inventory where last_restock = (select min(last_restock) from inventory where quantity is not null);
