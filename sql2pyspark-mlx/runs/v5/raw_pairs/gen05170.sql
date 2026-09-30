select id, city, age, email from (select *, dense_rank() over (partition by city order by age) row_num from customers) ranked where row_num <= 3
