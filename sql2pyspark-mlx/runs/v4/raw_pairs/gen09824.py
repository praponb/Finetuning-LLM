from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .withColumn("dept_name_length", F.length("dept_name"))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("DIRECT")))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("budget") >= 3159000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
        F.percentile_approx("dept_name_length", [0.25, 0.5, 0.75]).alias("dept_name_length_quartiles"),
    )
    .filter(F.col("num_dept_names") < 1)
)
