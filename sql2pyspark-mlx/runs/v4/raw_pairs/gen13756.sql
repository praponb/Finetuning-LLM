select id, status, method from payments where method in ('cash', 'card', 'promptpay') order by status, id desc
