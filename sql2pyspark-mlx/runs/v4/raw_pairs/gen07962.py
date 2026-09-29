from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("revenue_p50").desc())

valid = (
    df_store_sales
    .filter(F.col("store_id") == 2)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.add_months("sale_date", 3).alias("sale_date_plus_months"),
    )
)

result = (
    valid
    .groupBy(
        F.date_trunc("year", "sale_date_plus_months").alias("year_start"),
        F.date_trunc("week", "sale_date_plus_months").alias("week_start"),
    )
    .agg(
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
    )
    .withColumn("revenue_p50_rank", F.rank().over(w))
    .orderBy(F.col("year_start").desc_nulls_first())
)
