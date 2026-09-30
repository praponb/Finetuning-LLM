select is_active, case when age >= 57 then 'high' when age >= 27 then 'mid' else 'low' end as age_bucket, count(distinct age) as unique_countrys from customers group by 1, 2
