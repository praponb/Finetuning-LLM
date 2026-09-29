from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("cost").asc_nulls_first())
w5 = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price_avg_2_prior_per_cost").desc(), "id")
w6 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("order_id")
w8 = Window.partitionBy("product_id").orderBy("unit_price_bucket", "id").rowsBetween(-6, -1)

filtered = (
    df_order_items
    .filter(F.col("product_id").isin([6, 23, 36]))
    .select("*", (F.col("unit_price") - F.col("quantity")).alias("line_total"))
)

enriched = (
    filtered
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        F.col("d.category"),
        F.col("d.cost"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "category",
        "cost",
        F.ntile(5).over(w).alias("unit_price_bucket"),
        F.avg("unit_price").over(w2).alias("unit_price_avg_2_prior"),
        F.sum("unit_price").over(w3).alias("cumulative_unit_price"),
    )
)

base = (
    scored
    .filter(F.col("category").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "cost",
        "unit_price_bucket",
        "unit_price_avg_2_prior",
        F.round((F.col("unit_price_avg_2_prior") / F.col("cost")), 2).alias("unit_price_avg_2_prior_per_cost"),
        F.coalesce("category", F.lit("UNKNOWN")).alias("category_filled"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "order_id",
        "product_id",
        "cost",
        "unit_price_bucket",
        "unit_price_avg_2_prior",
        "category_filled",
        F.rank().over(w4).alias("cost_rank"),
        F.row_number().over(w5).alias("event_seq"),
        F.avg("unit_price_avg_2_prior_per_cost").over(w6).alias("unit_price_avg_2_prior_per_cost_avg_5"),
        F.max("unit_price_bucket").over(w_recent).alias("order_id_max_unit_price_bucket"),
        F.avg("unit_price_bucket").over(w8).alias("unit_price_bucket_avg_6_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "product_id",
        "cost",
        "unit_price_bucket",
        "unit_price_avg_2_prior",
        "category_filled",
        "cost_rank",
        "event_seq",
    )
)
