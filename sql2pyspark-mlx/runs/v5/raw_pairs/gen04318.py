from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .pivot("page", ["/checkout", "/product"])
    .agg(F.max("duration_sec").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "session_id",
        F.col("/checkout_value_sum").alias("checkout_value_sum"),
        F.col("/checkout_cnt").alias("checkout_cnt"),
        F.col("/product_value_sum").alias("product_value_sum"),
        F.col("/product_cnt").alias("product_cnt"),
    )
)
