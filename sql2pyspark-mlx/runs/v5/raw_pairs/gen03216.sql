select discount, amount, store_id, coalesce(channel, 'N/A') channel_filled from orders where channel = 'mobile' order by 3;
