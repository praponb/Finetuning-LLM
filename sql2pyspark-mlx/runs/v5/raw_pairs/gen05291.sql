select account_type, quarter(opened_date) as opened_quarter, sum(case when account_type = 'LOAN' then balance else 0 end) as loan_balance
from accounts
group by account_type, quarter(opened_date);
