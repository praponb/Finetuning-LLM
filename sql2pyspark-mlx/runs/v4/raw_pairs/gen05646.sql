select s.id, s.status, s.monthly_fee from subscriptions s where s.monthly_fee = (select min(s2.monthly_fee) from subscriptions s2 where s2.status = s.status) order by s.id desc;
