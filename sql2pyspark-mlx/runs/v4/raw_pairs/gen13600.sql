select status, 'account' source from accounts where customer_id = 10 union select status, 'payment' source from payments order by status
