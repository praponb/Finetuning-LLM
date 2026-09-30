select
    id,
    tags,
    cost,
    case when brand in ('Globex', 'Acme') then 'Tier 1' else 'Other' end as brand_group,
    coalesce(cost, 0) as cost_filled
from products
where launch_date > '2024-02-17' and category = 'Books' and cost < 460
