select 'product' as origin, price from products union all select 'subscription' as origin, monthly_fee from subscriptions order by price;
