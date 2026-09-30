from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_first())
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("brand").orderBy(F.col("running_revenue").desc(), "id")
w5 = Window.partitionBy("brand").orderBy(F.col("running_revenue").desc_nulls_first())
w6 = Window.partitionBy("brand", "product_id").orderBy(F.col("product_price").desc_nulls_last())

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-05-03")))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM-dd"))
)

with_windows = (
    filtered
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "sale_date_label",
        F.cume_dist().over(w).alias("units_cume_dist"),
        F.dense_rank().over(w2).alias("units_dense_rank"),
        F.sum("revenue").over(w3).alias("running_revenue"),
    )
)

enriched = (
    with_windows
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.product_id",
        "a.sale_date",
        "a.running_revenue",
        F.col("p.price"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.stock_qty").between(138, 219))
    .select(
        "s.id",
        "s.product_id",
        "s.sale_date",
        "s.running_revenue",
        F.col("p.brand"),
        F.col("p.price").alias("product_price"),
    )
)

scored = (
    joined
    .select(
        "id",
        "product_id",
        "sale_date",
        "brand",
        "product_price",
        F.row_number().over(w_hist).alias("event_seq"),
        F.cume_dist().over(w5).alias("running_revenue_cume_dist"),
        F.percent_rank().over(w6).alias("product_price_percent_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "sale_date",
        "product_price",
        "running_revenue_cume_dist",
        "product_price_percent_rank",
    )
    .orderBy(F.col("product_price").desc())
)
