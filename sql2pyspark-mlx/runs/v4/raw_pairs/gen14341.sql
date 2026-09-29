select p.id, p.category, p.cost from products p where p.cost = (select min(p2.cost) from products p2 where p2.category = p.category) order by p.id, p.cost desc;
