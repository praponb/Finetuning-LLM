with top_payments as (select *, dense_rank() over (partition by method order by amount) as row_num from payments) select * from top_payments where row_num <= 2
