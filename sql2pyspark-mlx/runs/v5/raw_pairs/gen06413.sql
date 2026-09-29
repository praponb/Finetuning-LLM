select carrier, avg_cost, max_cost
from (
    select carrier, avg(cost) avg_cost, max(cost) max_cost
    from shipments
    where carrier in ('Kerry', 'FedEx')
    group by carrier
) agg
where avg_cost > 54
order by avg_cost desc
