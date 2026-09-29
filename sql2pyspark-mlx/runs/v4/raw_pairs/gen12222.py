from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment")
w2 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("signup_date", "id")

with_windows = (
    df_customers
    .select(
        "*",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.sum("age").over(w2).alias("running_age"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

result = (
    with_windows
    .withColumn("running_age_tier", F.when(F.col("running_age") >= 251, "HIGH").otherwise("LOW"))
    .withColumn("credit_limit_share_tier", F.when(F.col("credit_limit_share") >= 0.85, "HIGH").otherwise("LOW"))
    .rollup("running_age_tier", "credit_limit_share_tier")
    .agg(
        F.stddev_samp("age").alias("age_stddev"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.grouping("running_age_tier").alias("grouping_id"),
    )
)
