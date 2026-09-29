with numbered as (select *, row_number() over (partition by account_type order by balance, id) as pos from accounts) select * from numbered where pos = 1 order by account_type asc
