from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(~F.col("location").isin(["Chiang Mai", "Singapore"]))
    .select("*", F.initcap(F.trim("dept_name")).alias("dept_name_title"))
)

result = (
    filtered
    .withColumn("budget_tier", F.when(F.col("budget") >= 3182000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["budget_tier", "dept_name"], ["budget_tier"], []],
        "budget_tier",
        "dept_name",
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_rows"),
    )
    .orderBy(F.col("budget_tier").asc_nulls_first())
)
