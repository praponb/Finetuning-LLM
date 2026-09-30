select * from (select country, segment, credit_limit from customers) pivot (count(credit_limit) for segment in ('Consumer', 'SMB'))
