select name, age, concat_ws('/', city, email) as city_email_key from customers where age between 38 and 60 or country = 'TH'
