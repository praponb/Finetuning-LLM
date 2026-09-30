with numbered as (
    select *, dense_rank() over (partition by status order by delivered_date desc) as rnk
    from shipments
    where carrier = 'DHL'
)
select *
from numbered
where rnk <= 3
