from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNull() & F.col("full_name").isNotNull())
    .groupBy("level")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L2", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l2"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .filter(F.col("pct_l2") < 35)
)
