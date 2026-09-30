select status, max(opened_date) as latest_opened_date
from accounts
group by status
