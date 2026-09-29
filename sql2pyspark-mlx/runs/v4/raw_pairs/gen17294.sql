with kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from orders s
    where not exists (
            select 1
            from payments ch
            where ch.order_id = s.id
              and ch.paid_at >= s.order_date
        )
)
-- Aggregate with conditional metrics
select
    case when shipping_fee >= 44 then 'PREMIUM' else 'STANDARD' end as shipping_fee_tier,
    count(*) filter (where channel = 'partner') as partner_rows,
    count(case when status = 'RETURNED' then 1 end) as returned_count,
    count(distinct status) as distinct_status
from kept
group by case when shipping_fee >= 44 then 'PREMIUM' else 'STANDARD' end
having returned_count >= 1
order by distinct_status desc nulls first;
