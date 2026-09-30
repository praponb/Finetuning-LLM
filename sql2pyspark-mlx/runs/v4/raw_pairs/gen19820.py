from pyspark.sql import functions as F

with_dims = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.cost"), F.col("b.price"))
)

result = (
    with_dims
    .withColumn("activity_month", F.date_trunc("MONTH", "sale_date"))
    .groupingSets(
        [["store_id", "activity_month"], ["store_id"], []],
        "store_id",
        "activity_month",
    )
    .agg(
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum("price").alias("total_price"),
        F.percentile_approx("price", 0.9).alias("price_p90"),
    )
    .orderBy("activity_month")
)
