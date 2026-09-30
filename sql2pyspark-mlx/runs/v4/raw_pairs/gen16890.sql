select opened_month, savings_count
from (
    select date_trunc('MONTH', opened_date) as opened_month, sum(case when account_type = 'SAVINGS' then 1 else 0 end) as savings_count
    from accounts
    group by date_trunc('MONTH', opened_date)
) as t
where savings_count >= 64
order by opened_month desc
limit 10;
