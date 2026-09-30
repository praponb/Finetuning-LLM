with top_shipments as (
    select *, row_number() over (partition by carrier order by delivered_date desc, id) as rn
    from shipments
)
select id, carrier, delivered_date
from top_shipments
where rn <= 1
order by 3 desc
