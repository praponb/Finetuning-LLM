select 'customer' as record_type, country, city from customers union select 'supplier' as record_type, country, city from suppliers where supplier_name is null order by city desc
