from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Chiang Mai", "New York", "Berlin"]))
    .groupBy(
        F.when(F.col("budget") >= 4423000, "high").when(F.col("budget") >= 1687000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("location").isin(["Chiang Mai", "New York"]), "core").otherwise("other").alias("location_group"),
    )
    .agg(F.max("budget").alias("max_budget"))
    .filter(F.col("max_budget") >= 2955000)
    .select("budget_bucket", "location_group", "max_budget")
    .orderBy(F.col("max_budget").desc(), F.col("location_group").desc())
)
