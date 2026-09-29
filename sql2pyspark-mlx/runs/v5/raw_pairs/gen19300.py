from pyspark.sql import functions as F

dim_totals = (
    df_employees
    .alias("e")
    .join(df_employees.alias("dim"), F.col("e.manager_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["employee_level", "employee_level"], ["employee_level"]],
        F.col("dim.level").alias("employee_level"),
        F.col("e.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("e.salary")), 2).alias("total_salary"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    dim_totals
    .select(
        "employee_level",
        "cnt",
        F.when(F.col("employee_level").rlike("^(L1|L3)$"), "GROUP_B").when(F.col("employee_level").rlike("^(L2|L4)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("employee_level_zone"),
    )
    .orderBy(F.col("employee_level_zone").desc_nulls_first())
)
