from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id", "store_id")
w_recent = Window.partitionBy("store_id")

valid = (
    df_store_sales
    .filter(F.col("revenue").between(2500, 3200))
    .withColumn("revenue_per_unit", F.round((F.col("units") / F.col("revenue")), 2))
)

base = (
    valid
    .filter(F.col("sale_date") >= F.lit("2023-07-04").cast("date"))
    .withColumn("sale_date_day", F.dayofmonth("sale_date"))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 1))
)

remaining_keys = (
    base
    .select("store_id", "id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units") == 34)
        .select("store_id", "id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.sum("units").over(w_part).alias("product_id_sum_units"),
        F.stddev_samp("sale_date_day").over(w_recent).alias("store_id_stddev_samp_sale_date_day"),
        F.avg("revenue_per_unit").over(w_recent).alias("store_id_avg_revenue_per_unit"),
    )
)

kept_keys = (
    enriched
    .select("product_id", "id")
    .exceptAll(
        df_store_sales
        .filter(F.col("sale_date") <= "2025-06-14")
        .select("product_id", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.price") >= 490)
    .select(
        "a.id",
        "a.product_id",
        "a.sale_date",
        "a.revenue_per_unit",
        "a.sale_date_day",
        "a.sale_date_plus_months",
        "a.product_id_sum_units",
        "a.store_id_stddev_samp_sale_date_day",
        F.col("d.cost"),
        F.col("d.category"),
    )
)

stats = (
    joined
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("activity_month"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.sum("revenue_per_unit") / F.nullif(F.count(F.when(F.col("category") == "Sports", 1)), F.lit(0)), 2).alias("revenue_per_unit_per_sports"),
    )
    .filter(F.col("revenue_per_unit_per_sports") < 1000000)
)

result = (
    stats
    .select("activity_month", "revenue_per_unit_per_sports")
    .orderBy(F.col("revenue_per_unit_per_sports").desc())
)
