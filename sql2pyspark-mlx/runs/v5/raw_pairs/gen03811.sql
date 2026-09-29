select * from stores where not (opened_date between '2025-01-01' and '2025-08-23' and city in ('Tokyo', 'Berlin')) order by region, city;
