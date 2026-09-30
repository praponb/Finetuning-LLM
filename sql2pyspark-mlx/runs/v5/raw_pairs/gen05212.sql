select quarter(start_date) as start_quarter, count(*) as row_count
from subscriptions
group by quarter(start_date);
