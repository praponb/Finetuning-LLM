select * from (select segment, country, age from customers) pivot (avg(age) for country in ('SG', 'UK', 'US', 'DE')) order by segment;
