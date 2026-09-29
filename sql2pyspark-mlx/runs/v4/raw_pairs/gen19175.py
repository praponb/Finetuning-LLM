from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("price", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("price").desc(), "id")
w3 = Window.orderBy("last_restock", "id")
w4 = Window.partitionBy("store_id", "product_id")

enriched = (
    df_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.price"))
)

scored = (
    enriched
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("price_bucket", F.ntile(10).over(w2))
    .withColumn("next_price", F.lead("price", 1).over(w3))
    .withColumn("store_id_avg_quantity", F.avg("quantity").over(w4))
)

remaining_keys = (
    scored
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("quantity").between(104, 211))
        .select("product_id", "id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("activity_month", F.date_trunc("MONTH", "last_restock"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 192, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["activity_month", "quantity_tier"], ["activity_month"], []],
        "activity_month",
        "quantity_tier",
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.min("next_price").alias("min_next_price"),
        F.min("price").alias("min_price"),
    )
)
