select email, signup_date
from customers
where country in ('SG', 'UK')
union
select email, signup_date
from customers
where country != 'TH'
order by email desc
