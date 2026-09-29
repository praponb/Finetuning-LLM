from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("amount_per_web"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)
