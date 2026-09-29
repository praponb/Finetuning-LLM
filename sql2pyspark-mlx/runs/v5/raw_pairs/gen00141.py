from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w_part = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_stores
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.sum("sqft").over(w).alias("region_sum_sqft"),
        F.last("sqft", ignorenulls=True).over(w_part).alias("last_known_sqft"),
    )
)

result = (
    scored
    .groupBy("region")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.count_if(F.col("region") == "East").alias("east_events"),
        F.percentile_approx("region_sum_sqft", [0.25, 0.5, 0.75]).alias("region_sum_sqft_quartiles"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
