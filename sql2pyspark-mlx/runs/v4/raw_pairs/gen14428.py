from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNull() & (F.col("location") == "Chiang Mai"))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("Unknown")))
)

budget_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3076000, "high").when(F.col("budget") >= 1772000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
)

result = budget_bucket_summary.orderBy("budget_bucket")
