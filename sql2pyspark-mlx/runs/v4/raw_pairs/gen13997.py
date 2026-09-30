from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

result = (
    with_last
    .withColumn("price_tier", F.when(F.col("price") >= 1030, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "store_sale_sale_date"))
    .groupingSets(
        [["price_tier", "activity_month"], ["price_tier"]],
        "price_tier",
        "activity_month",
    )
    .agg(
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
        F.percentile_approx("price", 0.5).alias("price_p50"),
    )
    .orderBy(F.col("sports_events").desc())
)
