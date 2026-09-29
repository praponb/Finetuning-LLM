with remaining_keys as (
    select carrier, status
    from shipments
    intersect all
    select carrier, status
    from shipments
    where status = 'IN_TRANSIT'
),
reconciled as (
    select s.*
    from shipments s
    join remaining_keys k on s.carrier = k.carrier and s.status = k.status
)
select
    date_trunc('week', shipped_date) as week_start,
    approx_percentile(cost, 0.95) as cost_p95,
    count(*) as cnt,
    round(sum(cost) / nullif(count(case when status = 'LOST' then 1 end), 0), 2) as cost_per_lost,
    sum(case when carrier = 'FedEx' then cost else 0 end) as fedex_cost
from reconciled
group by 1
having round(sum(cost) / nullif(count(case when status = 'LOST' then 1 end), 0), 2) > 0;
