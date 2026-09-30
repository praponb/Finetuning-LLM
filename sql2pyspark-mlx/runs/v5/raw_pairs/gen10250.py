from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        (F.col("pos") + 1).alias("line_no"),
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

result = (
    flattened
    .withColumn("hour_window", F.date_trunc("hour", "created_at"))
    .withColumn("price_tier", F.when(F.col("price") >= 234, "PREMIUM").otherwise("STANDARD"))
    .withColumn("line_no_tier", F.when(F.col("line_no") >= 3, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["hour_window", "price_tier", "line_no_tier"], ["hour_window", "price_tier"], []],
        "hour_window",
        "price_tier",
        "line_no_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("product_id", [0.25, 0.5, 0.75]).alias("product_id_quartiles"),
        F.sum("qty").alias("total_qty"),
    )
)
