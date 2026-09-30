from pyspark.sql import functions as F

with_dims = (
    df_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.brand") != "Acme")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.quantity",
        F.col("p.product_name"),
        F.col("p.brand"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("quantity") >= 182, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("quantity_per_globex"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("quantity")).otherwise(0)).alias("initech_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .orderBy("quantity_stddev")
)
