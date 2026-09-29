select * from (select *, rank() over (partition by country order by rating desc) as row_num from suppliers) as r where row_num = 1 order by id desc
