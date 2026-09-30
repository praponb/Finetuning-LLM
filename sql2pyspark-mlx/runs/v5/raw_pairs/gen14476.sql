select *, round(quantity, 2) as quantity_rounded from inventory where last_restock >= '2024-06-10';
