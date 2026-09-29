select id, sqft, dense_rank() over (order by sqft) as sqft_dense_rank from stores where region in ('Central', 'East', 'South');
