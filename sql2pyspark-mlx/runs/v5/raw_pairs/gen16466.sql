SELECT channel, id FROM promotions EXCEPT SELECT channel, id FROM promotions WHERE starts_on BETWEEN '2024-08-02' AND '2024-11-01';
