select * from (select *, row_number() over (partition by status order by monthly_fee desc nulls first, id) as latest_rank from subscriptions) ranked where latest_rank = 1
