select status, to_date(paid_at) as paid_day, min(paid_at) as first_paid_at
from payments
group by rollup(status, to_date(paid_at));
