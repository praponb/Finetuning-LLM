select status, round((100 * sum(case when method = 'paypal' then 1 else 0 end)) / count(*), 1) as pct_paypal, round((100 * sum(case when method = 'card' then 1 else 0 end)) / count(*), 1) as pct_card
from payments
where status = 'SUCCESS' and amount between 620 and 770 and method not in ('paypal', 'promptpay')
group by status;
