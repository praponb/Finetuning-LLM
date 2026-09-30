with numbered as (select *, dense_rank() over (partition by city order by credit_limit) as row_num from customers where is_active) select * from numbered where row_num = 1
