from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("brand")
w3 = Window.orderBy("price", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy(F.col("price_avg_2").asc_nulls_last())
w5 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

not_flagged = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.launch_date")), "left_anti")
)

enriched = (
    not_flagged
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "left")
    .filter(F.col("b.supplier_name").like("%Siam%"))
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.tags",
        "s.supplier_id",
        F.col("b.supplier_name"),
        F.col("b.rating"),
    )
)

valid = (
    enriched
    .filter(F.col("rating") >= 3.8)
    .select(
        "*",
        F.array_join("tags", "|").alias("tags_text"),
        F.initcap(F.trim("brand")).alias("brand_title"),
    )
)

scored = (
    valid
    .select(
        "id",
        "product_name",
        "price",
        "tags",
        "supplier_id",
        "rating",
        "brand_title",
        F.row_number().over(w).alias("row_seq"),
        F.count("rating").over(w2).alias("brand_count_rating"),
        F.avg("price").over(w3).alias("price_avg_2"),
    )
)

scored_products = (
    scored
    .select(
        "*",
        F.cume_dist().over(w4).alias("price_avg_2_cume_dist"),
        F.first("price_avg_2", ignorenulls=True).over(w5).alias("first_known_price_avg_2"),
    )
)

result = (
    scored_products
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL"))
    .cube("supplier_id", "row_seq_tier")
    .agg(
        F.sum("price_avg_2").alias("total_price_avg_2"),
        F.percentile_approx("brand_count_rating", 0.9).alias("brand_count_rating_p90"),
        F.percentile_approx("first_known_price_avg_2", 0.95).alias("first_known_price_avg_2_p95"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("supplier_id").desc_nulls_first())
)
