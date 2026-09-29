select
    city,
    signup_date,
    is_active,
    concat_ws('-', email, name) as email_name_key,
    last_day(signup_date) as signup_date_month_end
from customers
