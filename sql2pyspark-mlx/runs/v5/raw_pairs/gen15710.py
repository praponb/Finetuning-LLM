from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select("id", "customer_id", "created_at", F.col("elem.product_id"))
)

result = (
    flattened
    .groupBy(
        F.when(F.col("product_id") >= 11, "LARGE").otherwise("SMALL").alias("product_id_tier"),
    )
    .agg(
        F.percentile_approx("product_id", [0.25, 0.5, 0.75]).alias("product_id_quartiles"),
        F.count("*").alias("event_count"),
        F.min("product_id").alias("min_product_id"),
    )
    .filter(F.col("min_product_id") >= 0)
    .orderBy("event_count")
)
