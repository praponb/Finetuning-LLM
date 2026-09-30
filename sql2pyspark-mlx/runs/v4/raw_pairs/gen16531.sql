select case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end opened_cohort, year(opened_date) opened_year, avg(balance) avg_balance
from accounts
where account_type in ('CHECKING', 'CREDIT') and opened_date >= '2023-10-01'
group by opened_cohort, opened_year
having avg_balance > 19000
order by opened_year desc;
