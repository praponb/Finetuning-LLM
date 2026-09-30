select id, country, supplier_name from suppliers where country = 'UK' except select id, country, supplier_name from suppliers where supplier_name like 'S%' order by id desc
