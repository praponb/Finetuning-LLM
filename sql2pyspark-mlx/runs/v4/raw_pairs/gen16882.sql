select *, concat(segment, ', ', city) as segment_city, age + credit_limit as age_plus_credit_limit from customers where is_active and segment in ('Corporate', 'SMB') order by id limit 5;
