from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("avg_revenue").desc())

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-08-26").cast("date")) & F.col("store_id").isin([4, 5, 6]))
    .withColumn("sale_date_minus_57d", F.date_sub("sale_date", 57))
)

result = (
    filtered
    .groupBy(
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
        F.when(F.col("units") >= 40, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
    )
    .filter(F.col("units_bit_and") < 1)
    .withColumn("avg_revenue_rank", F.rank().over(w))
    .orderBy(F.col("units_tier").asc_nulls_last())
)
