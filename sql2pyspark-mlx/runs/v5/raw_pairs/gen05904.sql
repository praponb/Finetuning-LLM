SELECT channel, id FROM promotions INTERSECT ALL SELECT channel, id FROM promotions WHERE YEAR(ends_on) >= 2025;
