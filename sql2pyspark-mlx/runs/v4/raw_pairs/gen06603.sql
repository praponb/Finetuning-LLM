select id, sqft from stores where sqft >= (select avg(sqft) from stores where city in ('New York', 'Singapore', 'Berlin')) order by id;
