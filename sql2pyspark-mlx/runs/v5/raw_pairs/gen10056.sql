SELECT id, plan, monthly_fee FROM (SELECT id, plan, monthly_fee, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee, id) row_num FROM subscriptions) ranked WHERE row_num = 1 ORDER BY id DESC
