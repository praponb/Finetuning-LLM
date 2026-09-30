from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("product_id") >= 9, "LARGE").otherwise("SMALL").alias("product_id_tier"),
        F.when(F.col("qty") >= 4, "PREMIUM").otherwise("STANDARD").alias("qty_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("created_at").alias("started_at"),
        F.size(F.collect_set("promo")).alias("num_promos"),
    )
)
