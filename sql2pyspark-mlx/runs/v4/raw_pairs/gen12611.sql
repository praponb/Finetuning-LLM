select case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end signup_cohort, concat_ws(', ', sort_array(collect_set(city))) city_values
from customers
where email not like '%@GMAIL.COM' or city = 'Tokyo'
group by case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end
order by 2, signup_cohort
limit 10
