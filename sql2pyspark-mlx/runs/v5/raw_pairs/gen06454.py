from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("adjusted_revenue").asc_nulls_first(), "id")
w2 = Window.orderBy(F.col("revenue").desc())
w3 = Window.orderBy(F.col("price").desc_nulls_first())

cleaned = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-03-01", "2025-01-17"))
    .withColumn("adjusted_revenue", F.round((F.col("revenue") * 100), 2))
)

joined = (
    cleaned
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.product_name").like("La%"))
    .select("e.*", F.col("d.cost"), F.col("d.category"), F.col("d.price"))
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "sale_date", "units", "revenue", "cost", "price")
)

remaining_keys = (
    ranked
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([9, 10]))
        .select("id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter((F.col("sale_date") > F.lit("2023-06-11").cast("date")) & F.col("price").between(370, 850))
    .withColumn("margin", (F.col("cost") + F.col("price")))
)

with_windows = filtered.select("*", F.rank().over(w2).alias("revenue_rank"))

with_windows_store_sales = with_windows.withColumn("price_cume_dist", F.cume_dist().over(w3))

result = (
    with_windows_store_sales
    .groupBy(F.date_trunc("year", "sale_date").alias("year_start"))
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.bit_or("units").alias("units_bit_or"),
    )
    .orderBy(F.col("units_bit_or").desc_nulls_first())
)
