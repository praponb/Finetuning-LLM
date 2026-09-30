select month(opened_date) as opened_month, max(balance) as max_balance
from accounts
where customer_id in (6, 12, 20, 32) and balance is not null and customer_id in (15, 27, 40)
group by month(opened_date);
