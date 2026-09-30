from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_band", F.when(F.col("budget") >= 3405000, "High").when(F.col("budget") >= 1226000, "Medium").otherwise("Low"))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

result = (
    cleaned
    .groupBy("location")
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Finance", 1)), F.lit(0)), 2).alias("budget_per_finance"),
    )
)
