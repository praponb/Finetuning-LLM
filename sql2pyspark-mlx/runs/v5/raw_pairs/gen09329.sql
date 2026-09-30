select * from (select *, row_number() over (partition by category order by price desc, id) as row_num from products where price < 460) as ranked where row_num <= 3
