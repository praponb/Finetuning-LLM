from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .groupBy(
        F.month(F.col("s.opened_date")).alias("opened_month"),
        F.when(F.col("s.sqft") >= 17900, "high").when(F.col("s.sqft") >= 5151, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
)
