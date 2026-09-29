select id, age, country, upper(country) as country_upper from customers where age >= 33 order by age desc, id limit 20;
