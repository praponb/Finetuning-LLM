from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").asc_nulls_first(), "id")

latest = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("revenue") >= 3300, "LARGE").otherwise("SMALL").alias("revenue_tier"),
    )
    .agg(
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles_2"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
    )
    .filter(F.col("distinct_store_ids") > 1)
)
