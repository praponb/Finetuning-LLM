from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("brand").orderBy(F.col("revenue").desc_nulls_first(), "id")
w4 = Window.partitionBy("category").orderBy("tags_count", "id")
w5 = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")
w6 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("brand").eqNullSafe(F.lit("Acme"))))
    .withColumn("tags_count", F.size("tags"))
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity") == 6), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.tags_count",
        "s.dedup_rank",
        F.col("c.sale_date"),
        F.col("c.revenue"),
    )
)

ranked = (
    latest_match
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "tags_count",
        "sale_date",
        "revenue",
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "product_name",
        "category",
        "tags_count",
        "revenue",
        F.lead("revenue", 2).over(w4).alias("next_revenue"),
        F.ntile(10).over(w5).alias("price_bucket"),
        F.sum("cost").over(w6).alias("running_cost"),
    )
)

base = (
    with_windows
    .filter(F.col("revenue") > 1100)
    .select(
        "*",
        (F.col("price_bucket") + F.col("running_cost")).alias("price_bucket_plus_running_cost"),
    )
)

result = (
    base
    .select(
        "id",
        "category",
        "revenue",
        "next_revenue",
        "price_bucket_plus_running_cost",
    )
    .unpivot(
        ids=["id", "category"],
        values=["revenue", "next_revenue", "price_bucket_plus_running_cost"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
