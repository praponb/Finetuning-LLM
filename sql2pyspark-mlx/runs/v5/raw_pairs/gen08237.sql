select *
from (
    select case when amount >= 3100 then 'high' when amount >= 600 then 'mid' else 'low' end as amount_bucket, avg(amount) as avg_amount
    from transactions
    where amount > 1800
    group by case when amount >= 3100 then 'high' when amount >= 600 then 'mid' else 'low' end
) as agg
where avg_amount > 3200;
