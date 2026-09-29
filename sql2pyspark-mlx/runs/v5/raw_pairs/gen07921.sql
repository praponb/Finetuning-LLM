select *
from (
    select is_flagged, case when amount >= 3400 then 'high' when amount >= 1500 then 'mid' else 'low' end as amount_bucket, count(amount) as amount_count
    from transactions
    group by is_flagged, case when amount >= 3400 then 'high' when amount >= 1500 then 'mid' else 'low' end
) as t
where amount_count >= 8
order by amount_bucket
