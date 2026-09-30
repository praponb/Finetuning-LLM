select name, last_day(signup_date) signup_date_month_end from customers where lower(name) like '%lic%' and email is not null order by signup_date_month_end, name;
