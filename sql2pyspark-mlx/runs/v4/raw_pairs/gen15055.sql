select id, country, age, row_number() over (partition by country order by age desc, id) as age_row from customers
