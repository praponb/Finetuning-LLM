select balance, opened_date
from accounts
where account_type = 'SAVINGS' and balance >= 72000 and balance > 77000
