select id, brand, launch_date from (select *, row_number() over (partition by brand order by launch_date desc, id) as pos from products) as t where pos <= 3
