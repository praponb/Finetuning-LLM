select id, monthly_fee from subscriptions where monthly_fee <= (select min(monthly_fee) from subscriptions) order by monthly_fee;
