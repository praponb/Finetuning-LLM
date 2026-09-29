from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id")

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2024-01-14").cast("date")) & F.col("sale_date").between("2023-12-01", "2025-09-01"))
    .select("*", F.round("units", 1).alias("units_rounded"))
)

base = (
    filtered
    .filter(F.col("sale_date") >= F.lit("2024-04-01").cast("date"))
    .withColumn("units_whole", F.floor("units"))
)

enriched = (
    base
    .select(
        "*",
        F.first("units_rounded", ignorenulls=True).over(w).alias("first_known_units_rounded"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("YEAR", "sale_date").alias("year_start"), "product_id")
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.stddev_samp("units").alias("units_stddev"),
        F.percentile_approx("units_whole", [0.25, 0.5, 0.75]).alias("units_whole_quartiles"),
    )
    .orderBy(F.col("year_start").asc_nulls_last())
)
