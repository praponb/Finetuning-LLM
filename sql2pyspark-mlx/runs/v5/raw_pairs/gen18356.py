from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("price_tier", F.when(F.col("price") >= 1310, "LARGE").otherwise("SMALL"))
    .withColumn("cost_tier", F.when(F.col("cost") >= 720, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["price_tier", "cost_tier"], ["price_tier"], []],
        "price_tier",
        "cost_tier",
    )
    .agg(
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Umbrella", 1)), F.lit(0)), 2).alias("stock_qty_per_umbrella"),
        F.sum(F.when(F.col("category") == "Books", F.col("cost")).otherwise(0)).alias("books_cost"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("price_per_globex"),
        F.corr("cost", "price").alias("corr_cost_price"),
        F.grouping_id().alias("grouping_id"),
    )
)
