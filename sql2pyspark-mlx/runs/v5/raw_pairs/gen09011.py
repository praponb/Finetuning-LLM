from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_last(), "id")

valid = (
    df_products
    .filter(F.col("stock_qty") == 308)
    .select(
        "*",
        F.array_join("tags", ", ").alias("tags_text"),
        F.when(F.col("brand").isin(["Stark", "Globex"]), "Group A").otherwise("Other").alias("brand_group"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.category",
        "src.brand",
        "src.supplier_id",
        F.col("p.country"),
        F.col("p.city"),
        F.col("p.rating"),
    )
)

latest = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") <= "2025-07-01"), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity") >= 7), "left_anti")
)

enriched = (
    kept
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.supplier_id",
        "e.country",
        "e.city",
        F.col("b.rating").alias("supplier_rating"),
        F.col("b.city").alias("supplier_city"),
    )
)

result = (
    enriched
    .groupBy("country")
    .agg(
        F.round(F.avg("supplier_rating"), 2).alias("avg_supplier_rating"),
        F.sum(F.when(F.col("country") == "US", F.col("supplier_rating")).otherwise(0)).alias("us_supplier_rating"),
        F.count_if(F.col("city") == "London").alias("london_events"),
    )
)
