select id, shipping_fee, rank() over (order by shipping_fee) as shipping_fee_rank from orders;
