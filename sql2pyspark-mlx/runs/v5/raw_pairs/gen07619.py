from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("units_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("running_max_units_filled", "id")
w3 = Window.orderBy("event_seq", "id").rowsBetween(-6, -1)
w4 = Window.orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("units", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-06-21")))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "sale_date",
        "units",
        "units_filled",
        F.min("units_filled").over(w_part).alias("running_min_units_filled"),
        F.avg("units_filled").over(w_part).alias("running_avg_units_filled"),
        F.max("units_filled").over(w_part).alias("running_max_units_filled"),
    )
)

with_windows_store_sales = (
    with_windows
    .select(
        "id",
        "sale_date",
        "units",
        "running_max_units_filled",
        F.lead("running_avg_units_filled").over(w2).alias("next_running_avg_units_filled"),
        F.lead("running_max_units_filled").over(w2).alias("next_running_max_units_filled"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

enriched = (
    with_windows_store_sales
    .withColumn("next_running_avg_units_filled_avg_6_prior", F.avg("next_running_avg_units_filled").over(w3))
    .withColumn("next_running_avg_units_filled_avg_2", F.avg("next_running_avg_units_filled").over(w4))
    .withColumn("next_next_running_max_units_filled", F.lead("next_running_max_units_filled", 1, 0).over(w5))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
        F.when(F.col("next_running_avg_units_filled_avg_6_prior") >= 83, "LARGE").otherwise("SMALL").alias("next_running_avg_units_filled_avg_6_prior_tier"),
        F.when(F.col("next_running_max_units_filled") >= 81, "LARGE").otherwise("SMALL").alias("next_running_max_units_filled_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("next_running_avg_units_filled_avg_2"), 2).alias("avg_next_running_avg_units_filled_avg_2"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc_nulls_last())
)
