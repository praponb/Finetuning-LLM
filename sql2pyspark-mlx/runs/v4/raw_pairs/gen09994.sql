select * from (select *, rank() over (partition by supplier_id order by cost nulls first) as rank_in_group from products) x where rank_in_group = 1;
