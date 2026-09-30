select discount, channel, status, substring(channel, 1, 4) as channel_prefix from orders where shipping_fee >= 21 order by discount desc
