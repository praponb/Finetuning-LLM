from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id")
w3 = Window.orderBy(F.col("units").desc_nulls_last())
w4 = Window.orderBy(F.col("units").desc_nulls_first())
w5 = Window.partitionBy("brand", "store_id").orderBy(F.col("corr_units_revenue").desc_nulls_last())
w6 = Window.orderBy(F.col("revenue_avg_6").asc_nulls_last())
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

metrics = (
    df_store_sales
    .select(
        "*",
        F.avg("revenue").over(w).alias("revenue_avg_6"),
        F.corr("units", "revenue").over(w2).alias("corr_units_revenue"),
    )
)

enriched = (
    metrics
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.brand"), F.col("d.price"))
)

remaining_keys = (
    enriched
    .select("product_id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([3, 6, 7, 10]))
        .select("product_id", "store_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("brand").isNotNull())
    .withColumn("sale_date_minus_32d", F.date_sub("sale_date", 32))
    .withColumn("brand_prefix", F.substring("brand", 1, 4))
)

with_windows = (
    base
    .select(
        "*",
        F.corr("revenue", "units").over(w2).alias("corr_revenue_units"),
        F.percent_rank().over(w3).alias("units_percent_rank"),
        F.rank().over(w4).alias("units_rank"),
        F.rank().over(w5).alias("corr_units_revenue_rank"),
        F.rank().over(w6).alias("revenue_avg_6_rank"),
    )
)

result = (
    with_windows
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "revenue_avg_6",
        "corr_units_revenue",
        "brand",
        "price",
        "sale_date_minus_32d",
        F.col("avg_store_sale_revenue"),
    )
)
