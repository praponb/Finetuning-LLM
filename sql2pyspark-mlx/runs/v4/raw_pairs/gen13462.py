from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "brand").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand", "product_id").orderBy("last_restock", "id").rowsBetween(-3, -1)
w_hist = Window.partitionBy("store_id", "brand").orderBy("last_restock", "id")
w4 = Window.partitionBy("store_id").orderBy("last_restock", "id")
w5 = Window.partitionBy("brand", "product_id").orderBy(F.col("seq_no").desc_nulls_last(), "id")
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

with_dims = (
    df_inventory
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.brand"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.first("brand", ignorenulls=True).over(w).alias("first_known_brand"),
        F.avg("quantity").over(w2).alias("quantity_avg_3_prior"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

valid = (
    ranked
    .filter(F.col("quantity") >= 198)
    .select(
        "id",
        "store_id",
        "product_id",
        "first_known_brand",
        "event_seq",
        "seq_no",
        "rn",
        F.coalesce("quantity_avg_3_prior", F.lit(0)).alias("quantity_avg_3_prior_filled"),
    )
)

result = (
    valid
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "first_known_brand",
        "event_seq",
        "rn",
        F.col("avg_store_sale_revenue"),
    )
)
