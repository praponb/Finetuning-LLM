from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("city").orderBy("units", "id")
w4 = Window.orderBy("units", "id")
w_recent_2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-08").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.col("sqft").cast("string").alias("sqft_str"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"), F.col("c.units"))
)

with_windows = (
    latest_match
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("seq_no_2", F.row_number().over(w4))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w_recent_2))
)

result = (
    with_windows
    .groupBy("region", F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.percentile_approx("units", 0.5).alias("units_p50"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("seq_no_per_tokyo"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("region") == "South", 1)), F.lit(0)), 2).alias("seq_no_per_south"),
    )
    .filter(F.col("units_p50") >= 0)
    .orderBy("units_p50")
)
