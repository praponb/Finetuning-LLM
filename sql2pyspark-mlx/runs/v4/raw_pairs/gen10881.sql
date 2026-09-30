select * from (select *, row_number() over (partition by region order by opened_date desc, id) as row_num from stores) ranked where row_num = 1;
