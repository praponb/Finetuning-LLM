from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("manager_id", "id")
    .exceptAll(
        df_employees
        .filter(F.col("salary").between(64000, 78000))
        .select("manager_id", "id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .rollup(
        F.when(F.col("salary") >= 146000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
        "dept_id",
    )
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum("salary").alias("total_salary"),
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
    )
    .orderBy(F.col("avg_salary").desc_nulls_last())
)
