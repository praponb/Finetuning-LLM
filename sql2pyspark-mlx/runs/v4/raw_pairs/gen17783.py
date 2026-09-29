from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category").orderBy(F.col("launch_date").asc_nulls_last(), "id")
w_recent = Window.orderBy("cost", "id")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")

ranked = (
    df_products
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .select(
        "*",
        F.ntile(10).over(w_recent).alias("cost_bucket"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

filtered = (
    enriched
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-06-01")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "cost_bucket",
        F.round("price", 2).alias("price_rounded"),
    )
)

result = (
    filtered
    .select("id", "category", "price", "price_rounded")
    .unpivot(["id", "category"], ["price", "price_rounded"], "kpi", "metric_value")
    .filter(F.col("metric_value").isNotNull())
)
