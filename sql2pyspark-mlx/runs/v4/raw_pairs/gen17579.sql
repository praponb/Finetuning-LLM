with top_shipments as (
    select
        id,
        carrier,
        cost,
        delivered_date,
        row_number() over (partition by carrier order by cost desc, id) as rnk
    from shipments
    where shipped_date between '2024-05-01' and '2025-05-01'
)
select
    id,
    carrier,
    cost,
    delivered_date
from top_shipments
where rnk <= 3
order by delivered_date desc, carrier
