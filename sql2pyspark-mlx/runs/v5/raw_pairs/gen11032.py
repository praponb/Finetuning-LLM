from pyspark.sql import functions as F

src = (
    df_employees
    .filter((F.lower(F.trim("email")) == "frank@company.co") & (F.col("salary") > 96000))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)

level_summary = (
    src
    .groupBy("level")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
)

result = level_summary
