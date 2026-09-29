select id, carrier, delivered_date
from (
    select
        id,
        carrier,
        delivered_date,
        row_number() over (partition by carrier order by delivered_date desc, id) as pos
    from shipments
) as r
where pos <= 5
order by delivered_date asc;
