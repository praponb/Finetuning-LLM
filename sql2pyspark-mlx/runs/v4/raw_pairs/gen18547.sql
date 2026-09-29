select id, quantity from inventory where quantity > (select avg(quantity) from inventory where last_restock <= '2023-09-01')
