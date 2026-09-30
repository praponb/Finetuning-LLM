select hire_date as event_date from employees intersect select last_restock as event_date from inventory where product_id in (2, 3, 10);
