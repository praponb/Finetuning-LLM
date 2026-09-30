select
    id,
    account_type,
    opened_date,
    rank() over (partition by account_type order by opened_date desc) as rnk
from accounts
