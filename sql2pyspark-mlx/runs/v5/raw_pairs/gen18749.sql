with Numbered as (select *, row_number() over (partition by account_type order by balance, id) as row_num from accounts) select * from Numbered where row_num <= 3 order by id asc
