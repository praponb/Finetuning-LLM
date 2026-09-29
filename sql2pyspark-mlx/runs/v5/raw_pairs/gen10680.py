from pyspark.sql import functions as F

rollup_totals = (
    df_employees
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .rollup(F.col("d.dept_name").alias("dept_name"), F.col("src.level").alias("level"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("src.salary")).alias("total_salary"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    rollup_totals
    .select(
        "level",
        "total_salary",
        "gid",
        F.when(F.col("level").rlike("^(L1|L2)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("level_zone"),
    )
    .orderBy(F.col("gid").desc_nulls_last())
)
