select *, lower(carrier) as carrier_lower
from shipments
where shipped_date <= '2025-05-01' and carrier in ('FedEx', 'Flash');
