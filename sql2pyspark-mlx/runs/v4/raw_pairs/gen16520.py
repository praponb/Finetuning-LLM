from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-3, -1)
w_running_2 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("revenue_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("units", "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_last())

with_windows = (
    df_store_sales
    .withColumn("running_units", F.sum("units").over(w_running))
    .withColumn("revenue_avg_3_prior", F.avg("revenue").over(w2))
    .withColumn("units_avg_3", F.avg("units").over(w_running_2))
)

scored = (
    with_windows
    .select(
        "*",
        F.first("running_units", ignorenulls=True).over(w4).alias("first_known_running_units"),
        F.row_number().over(w5).alias("event_seq"),
        F.dense_rank().over(w6).alias("units_dense_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("sale_date") >= F.lit("2023-06-22").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "running_units",
        "revenue_avg_3_prior",
        "first_known_running_units",
        "event_seq",
        "units_dense_rank",
        F.round((F.col("units_dense_rank") * 100), 2).alias("units_dense_rank_adjusted"),
    )
)

valid = (
    filtered
    .filter(F.col("units_dense_rank_adjusted") >= 63)
    .select(
        "*",
        F.when(F.col("units_dense_rank") >= 8, "High").when(F.col("units_dense_rank") >= 4, "Medium").otherwise("Low").alias("units_dense_rank_tier"),
    )
)

with_dims = (
    valid
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.sqft"), F.col("p2.opened_date"))
)

remaining_keys = (
    with_dims
    .select("store_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("units") > 38)
        .select("store_id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "running_units",
        "event_seq",
        "units_dense_rank",
        "units_dense_rank_adjusted",
        "units_dense_rank_tier",
    )
    .orderBy(F.col("event_seq").desc())
)
