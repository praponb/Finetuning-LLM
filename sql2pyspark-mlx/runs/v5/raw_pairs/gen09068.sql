with filtered_shipments as (
    select *
    from shipments
    where shipped_date >= '2025-04-18' and shipped_date between '2024-10-01' and '2025-09-01' and carrier in ('FedEx', 'Kerry')
),
summary as (
    select carrier, max(shipped_date) last_shipped_date
    from filtered_shipments
    group by carrier
)
select carrier, last_shipped_date
from summary
order by last_shipped_date desc, carrier asc;
