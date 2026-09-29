with kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from suppliers s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.supplier_id in (10, 16, 27, 32)
        )
)
-- Grouped metrics
select
    case when rating >= 4.3 then 'PREMIUM' else 'STANDARD' end as rating_tier,
    round(avg(rating), 2) as avg_rating,
    sum(rating) filter (where country = 'JP') as jp_total_rating,
    round(avg(rating), 2) as avg_rating_2,
    count(*) filter (where country = 'UK') as uk_rows
from kept
group by rating_tier
having uk_rows < 1000
order by avg_rating desc nulls first
