with numbered as (select id, status, monthly_fee, row_number() over (partition by status order by monthly_fee, id) as rnk from subscriptions) select * from numbered where rnk <= 5 order by id desc;
