from pyspark.sql import functions as F

with_dims = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.opened_date"))
)

result = (
    with_dims
    .groupBy(F.date_trunc("QUARTER", "last_restock").alias("quarter_start"))
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.sum("quantity").alias("total_quantity"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .filter(F.col("quantity_p90") < 0)
    .orderBy(F.col("quantity_p90").desc_nulls_last())
)
