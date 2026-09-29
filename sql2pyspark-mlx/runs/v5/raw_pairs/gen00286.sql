select dept_name, max(budget) - min(budget) as budget_range from departments group by dept_name having max(budget) - min(budget) > 75;
