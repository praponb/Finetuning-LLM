SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) AS rank_in_group FROM departments QUALIFY rank_in_group = 1;
