from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("channel") == "mobile")
    .select("*", F.col("amount").cast("int").alias("amount_int"))
)

result = (
    valid
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.percentile_approx("amount_int", [0.25, 0.5, 0.75]).alias("amount_int_quartiles"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
    )
    .orderBy(F.col("web_rows").desc_nulls_last())
)
