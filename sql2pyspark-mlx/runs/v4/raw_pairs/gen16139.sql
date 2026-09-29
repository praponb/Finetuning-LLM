select
    id,
    customer_id,
    started_at,
    quarter(started_at) as started_at_quarter
from sessions
where country in ('JP', 'TH', 'DE') and country = 'DE' and browser is null;
