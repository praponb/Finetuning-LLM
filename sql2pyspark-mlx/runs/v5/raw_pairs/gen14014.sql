select *, rank() over (partition by order_id, status order by paid_at desc) as rank_in_group from payments qualify rank_in_group = 1;
