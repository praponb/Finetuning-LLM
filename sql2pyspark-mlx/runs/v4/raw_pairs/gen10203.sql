select
    cost,
    shipped_date,
    lower(status) as status_lower,
    substring(carrier, 1, 4) as carrier_prefix
from shipments
where carrier = 'Kerry';
