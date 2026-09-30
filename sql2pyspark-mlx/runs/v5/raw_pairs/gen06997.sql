select status, 'order' as origin from orders where discount > 0.26 union all select status, 'payment' as origin from payments where method in ('promptpay', 'card', 'bank_transfer') order by status;
