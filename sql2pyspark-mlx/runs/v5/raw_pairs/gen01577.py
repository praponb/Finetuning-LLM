from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w5 = Window.orderBy(F.col("units").desc())
w6 = Window.orderBy("next_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("next_revenue").desc_nulls_first())
w8 = Window.orderBy(F.col("running_revenue_num").desc_nulls_first())
w9 = Window.orderBy("last_known_units")

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-04-29").cast("date")) & (F.col("revenue") <= 2300))
    .select("*", (F.col("revenue") - F.col("units")).alias("revenue_per_unit"))
)

metrics = (
    valid
    .filter(F.col("product_id").isin([1, 5, 8, 10]))
    .select(
        "id",
        "units",
        "revenue",
        F.last("units", ignorenulls=True).over(w).alias("last_known_units"),
        F.last("revenue_per_unit", ignorenulls=True).over(w2).alias("last_known_revenue_per_unit"),
        F.avg("units").over(w3).alias("units_avg_6_prior"),
        F.lead("revenue", 1).over(w4).alias("next_revenue"),
        F.rank().over(w5).alias("units_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "revenue",
        "last_known_units",
        "units_rank",
        F.sum("revenue").over(w6).alias("running_revenue"),
        F.dense_rank().over(w_running).alias("next_revenue_dense_rank"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id").isin([1, 3, 4, 6]))
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("units_rank") > 6)
    .select(
        "id",
        "revenue",
        "last_known_units",
        "units_rank",
        "next_revenue_dense_rank",
        F.col("running_revenue").cast("double").alias("running_revenue_num"),
    )
)

cleaned = (
    base
    .filter(F.col("last_known_units") != 12)
    .select(
        "id",
        "revenue",
        "last_known_units",
        "units_rank",
        "next_revenue_dense_rank",
        "running_revenue_num",
        F.round((F.col("revenue") * 1.15), 2).alias("adjusted_revenue"),
        F.round((F.col("revenue") / F.col("last_known_units")), 2).alias("revenue_per_last_known_units"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "revenue",
        "last_known_units",
        "units_rank",
        "running_revenue_num",
        F.dense_rank().over(w8).alias("running_revenue_num_dense_rank"),
        F.cume_dist().over(w9).alias("last_known_units_cume_dist"),
    )
)

result = (
    scored
    .select(
        "id",
        "revenue",
        "last_known_units",
        "units_rank",
        "running_revenue_num",
        "running_revenue_num_dense_rank",
        "last_known_units_cume_dist",
    )
    .orderBy(F.col("units_rank").desc(), F.col("revenue").desc())
)
