from pyspark.sql import functions as F

joined = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.store_name").like("%Mal%"))
    .select("s.*", F.col("b.store_name"), F.col("b.opened_date"), F.col("b.region"))
)

result = (
    joined
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
        "region",
        F.when(F.col("quantity") >= 262, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
)
