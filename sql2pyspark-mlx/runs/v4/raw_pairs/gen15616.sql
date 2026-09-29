select * from customers qualify row_number() over (partition by city, country order by age desc nulls last, id) = 1
