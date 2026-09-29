select * from (select *, row_number() over (partition by status, account_type order by balance, id) as row_num from accounts) t where row_num = 1;
