select id, cost, category from products where cost >= (select max(cost) from products) order by category
