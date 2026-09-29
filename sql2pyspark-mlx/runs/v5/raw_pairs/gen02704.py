from pyspark.sql import functions as F

base = (
    df_departments
    .filter((~F.col("location").eqNullSafe(F.lit("Chiang Mai"))) & (F.col("budget") < 1672000))
    .select(
        "*",
        F.when(F.col("location").isin(["Bangkok", "Berlin"]), "Priority").otherwise("Other").alias("location_group"),
        F.round((F.col("budget") * 0.85), 2).alias("budget_scaled"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_filled"),
    )
)

result = (
    base
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "Tokyo"])
    .agg(F.avg("budget_scaled").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "dept_name",
        F.col("Chiang Mai_value_sum").alias("chiang_mai_value_sum"),
        F.col("Chiang Mai_n").alias("chiang_mai_n"),
        F.col("Tokyo_value_sum").alias("tokyo_value_sum"),
        F.col("Tokyo_n").alias("tokyo_n"),
    )
)
