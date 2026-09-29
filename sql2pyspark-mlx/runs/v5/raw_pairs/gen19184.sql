select a.id, a.account_type, a.opened_date
from accounts a
where a.customer_id not in (select id from customers where credit_limit < 13700)
