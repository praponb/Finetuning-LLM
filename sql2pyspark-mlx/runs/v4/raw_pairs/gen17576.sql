with numbered as (select *, row_number() over (partition by customer_id order by start_date desc, id) as row_num from subscriptions) select * from numbered where row_num = 1
