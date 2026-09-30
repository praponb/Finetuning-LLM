select c.name, s.started_at, s.device, s.customer_id from customers c left join sessions s on s.customer_id = c.id where c.segment in ('Consumer', 'Corporate', 'SMB');
