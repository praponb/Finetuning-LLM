from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id", "product_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_last(), "id")

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-03-31"))) & (F.year("sale_date") == 2024))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.date_trunc("month", "sale_date").alias("sale_date_period"),
    )
)

enriched = (
    valid
    .filter(F.col("revenue") > 2700)
    .withColumn("running_sum_units", F.sum("units").over(w_recent))
    .withColumn("running_min_units", F.min("units").over(w_recent))
    .withColumn("running_count_units", F.count("units").over(w_recent))
    .withColumn("running_avg_units", F.avg("units").over(w_recent))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    deduped
    .select("product_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("units") > 16)
        .select("product_id", "id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter((F.col("sale_date") > F.lit("2023-04-27").cast("date")) & F.col("store_id").isin([1, 2, 5, 6]))
    .withColumn("running_count_units_times_running_min_units", (F.col("running_count_units") * F.col("running_min_units")))
)

result = (
    base
    .select(
        "store_id",
        "product_id",
        "sale_date",
        "sale_date_period",
        "running_sum_units",
        "running_count_units",
        "running_avg_units",
        "rank_in_group",
        "running_count_units_times_running_min_units",
    )
    .orderBy("store_id", F.col("running_avg_units").desc())
)
