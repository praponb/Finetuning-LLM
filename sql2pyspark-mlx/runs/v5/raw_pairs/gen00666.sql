select * from (select country, segment, age from customers where email is not null) pivot (avg(age) for segment in ('SMB', 'Consumer', 'Corporate'))
