from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id", "brand").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())
w3 = Window.partitionBy("category", "brand").orderBy("prev_price", "id")
w_part = Window.partitionBy("brand").orderBy("revenue_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("revenue_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.sale_date",
        "a.revenue",
        F.col("p.price"),
        F.col("p.brand"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.lead("revenue").over(w_running).alias("next_revenue"),
        F.lead("price").over(w_running).alias("next_price"),
        F.lag("price").over(w_running).alias("prev_price"),
        F.row_number().over(w_running).alias("event_seq"),
        F.cume_dist().over(w2).alias("revenue_cume_dist"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.price").alias("product_price"), F.col("b.category"))
)

valid = (
    with_dims
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-07-24"))) & F.col("category").isNotNull())
    .withColumn("prev_price_band", F.when(F.col("prev_price") >= 1180, "High").when(F.col("prev_price") >= 180, "Medium").otherwise("Low"))
)

scored = (
    valid
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "brand",
        "next_revenue",
        "next_price",
        "prev_price",
        "event_seq",
        "revenue_cume_dist",
        "product_price",
        "prev_price_band",
        F.lead("event_seq", 2, 0).over(w3).alias("next_event_seq"),
        F.last("next_revenue", ignorenulls=True).over(w_part).alias("last_known_next_revenue"),
        F.last("event_seq", ignorenulls=True).over(w5).alias("last_known_event_seq"),
    )
)

result = (
    scored
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.category").alias("category"),
        F.col("d.brand").alias("product_brand"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.revenue")).alias("max_revenue"),
    )
    .orderBy(F.col("rows_total").desc_nulls_first())
)
