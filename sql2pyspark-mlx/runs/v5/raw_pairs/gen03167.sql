select status from orders intersect select status from accounts where balance between 23000 and 75000 order by status desc;
