select is_active, upper(segment) as segment_upper, credit_limit * age as credit_limit_times_age
from customers
where is_active or length(email) < 11
