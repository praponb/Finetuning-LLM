select * from (select segment, city, country, credit_limit from customers) src pivot (avg(credit_limit) as value_sum, count(1) as n for country in ('JP', 'UK'))
