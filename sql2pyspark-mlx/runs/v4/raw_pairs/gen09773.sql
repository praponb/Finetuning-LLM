select id, method
from payments
where (method in ('promptpay', 'cash') and amount <= 1100) or amount > 1490
