select * from (select city, country, segment, credit_limit from customers) src pivot (sum(credit_limit) as total, count(1) as item_count for segment in ('Corporate', 'Consumer', 'SMB'))
