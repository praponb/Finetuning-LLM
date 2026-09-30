select status
from accounts
where status != 'FROZEN'
except
select status
from payments
where to_date(paid_at) >= '2025-10-28'
