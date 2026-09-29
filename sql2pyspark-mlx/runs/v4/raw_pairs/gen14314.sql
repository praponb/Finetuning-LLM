with ranked_shipments as (
    select
        id,
        carrier,
        shipped_date,
        row_number() over (partition by carrier order by shipped_date desc, id) as rn
    from shipments
)
select id, carrier, shipped_date
from ranked_shipments
where rn <= 3
order by shipped_date;
