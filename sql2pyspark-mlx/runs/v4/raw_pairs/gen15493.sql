select * from (select city, country, age from customers)
pivot (avg(age) for country in ('US', 'JP', 'UK'))
order by city;
