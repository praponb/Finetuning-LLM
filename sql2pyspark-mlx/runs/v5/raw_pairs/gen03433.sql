select
    id,
    carrier,
    status,
    cost,
    round(cost / sum(cost) over (partition by carrier, status), 3) as cost_share
from shipments
order by cost_share desc;
