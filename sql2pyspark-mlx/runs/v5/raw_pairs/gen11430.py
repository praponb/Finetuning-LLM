from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_last())
w3 = Window.orderBy(F.col("revenue_percent_rank").desc_nulls_last())
w4 = Window.orderBy(F.col("cost").desc_nulls_last())
w5 = Window.orderBy(F.col("revenue_dense_rank").asc_nulls_last())

kept_keys = (
    df_store_sales
    .select("product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("revenue").between(1100, 2100))
        .select("product_id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter((F.col("sale_date") > F.lit("2023-09-18").cast("date")) & F.month("sale_date").isin([4, 8, 9]))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
)

joined = (
    cleaned
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .filter(F.col("b.stock_qty") != 207)
    .select("e.id", "e.product_id", "e.sale_date", "e.revenue", F.col("b.cost"))
)

scored = (
    joined
    .select(
        "sale_date",
        "cost",
        F.dense_rank().over(w).alias("revenue_dense_rank"),
        F.percent_rank().over(w2).alias("revenue_percent_rank"),
    )
)

with_windows = (
    scored
    .withColumn("revenue_percent_rank_cume_dist", F.cume_dist().over(w3))
    .withColumn("cost_dense_rank", F.dense_rank().over(w4))
    .withColumn("revenue_dense_rank_dense_rank", F.dense_rank().over(w5))
)

result = (
    with_windows
    .cube(
        F.date_trunc("year", "sale_date").alias("year_start"),
        F.when(F.col("revenue_dense_rank_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("revenue_dense_rank_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("revenue_dense_rank_dense_rank_tier").desc())
)
