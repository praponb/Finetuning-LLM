select case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end opened_cohort, customer_id, max(balance) max_balance
from accounts
group by opened_cohort, customer_id
having max_balance < 33000
