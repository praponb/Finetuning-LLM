from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 130000) & ((F.col("hire_date") < "2025-04-07") | F.col("is_remote")))
    .groupBy(
        F.when(F.col("level").isin(["L1", "L3"]), "tier_1").otherwise("other").alias("level_group"),
        "is_remote",
    )
    .agg(F.min("salary").alias("min_salary"))
    .orderBy("level_group", "is_remote")
    .limit(3)
)
