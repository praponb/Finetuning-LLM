select id, country, signup_date from (select id, country, signup_date, row_number() over (partition by country order by signup_date, id) as row_num from customers) as ranked where row_num <= 1;
