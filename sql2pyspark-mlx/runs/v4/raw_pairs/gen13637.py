from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w_part = Window.partitionBy("store_id").orderBy(F.col("sale_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())
w5 = Window.orderBy(F.col("sale_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("store_id")
w7 = Window.partitionBy("city")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-12-08"))) & F.col("sale_date").between("2025-06-01", "2025-10-18"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.date_trunc("YEAR", "sale_date").alias("sale_date_period"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

kept_keys = (
    cleaned
    .select("product_id", "store_id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue") >= 3800)
        .select("product_id", "store_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.count("units").over(w).alias("product_id_count_units"),
        F.count("units").over(w_part).alias("units_count_30d"),
        F.round((F.col("units") / F.sum("units").over(w3)), 4).alias("units_share"),
        F.percent_rank().over(w4).alias("revenue_percent_rank"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.store_name"), F.col("d.city"))
)

matched_keys = (
    enriched
    .select("store_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("revenue") < 1500)
        .select("store_id")
    )
)

kept_rows_store_sales = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

scored_store_sales = (
    kept_rows_store_sales
    .withColumn("units_share_avg_7d", F.avg("units_share").over(w5))
    .withColumn("store_id_count_units_share", F.count("units_share").over(w6))
    .withColumn("city_sum_units_count_30d", F.sum("units_count_30d").over(w7))
)

result = (
    scored_store_sales
    .groupBy("product_id")
    .pivot("city", ["London", "Tokyo"])
    .agg(F.sum("units"))
)
