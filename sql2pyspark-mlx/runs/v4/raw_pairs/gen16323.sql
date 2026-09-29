with numbered as (select *, row_number() over (partition by channel order by order_date, id) as rnk from orders) select id, channel, order_date from numbered where rnk <= 5;
