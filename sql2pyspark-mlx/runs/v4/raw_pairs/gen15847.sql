select distinct account_type, status
from accounts
order by status desc, account_type desc
limit 20;
