from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("running_max_sqft", "id")
w_grp = Window.orderBy(F.col("running_sum_sqft").desc_nulls_first())
w_part = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("running_avg_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_stores
    .select(
        "id",
        "sqft",
        F.count("sqft").over(w).alias("running_count_sqft"),
        F.max("sqft").over(w).alias("running_max_sqft"),
        F.sum("sqft").over(w).alias("running_sum_sqft"),
        F.avg("sqft").over(w).alias("running_avg_sqft"),
    )
)

enriched = (
    scored
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("running_sum_sqft_cume_dist", F.cume_dist().over(w_grp))
)

metrics = (
    enriched
    .filter(F.col("running_sum_sqft_cume_dist") < 0.57)
    .select(
        "id",
        "running_count_sqft",
        "running_max_sqft",
        "row_seq",
        "running_sum_sqft_cume_dist",
        F.max("running_avg_sqft").over(w_part).alias("running_max_running_avg_sqft"),
        F.sum("running_avg_sqft").over(w_part).alias("running_sum_running_avg_sqft"),
        F.avg("running_avg_sqft").over(w_part).alias("running_avg_running_avg_sqft"),
        F.first("running_sum_sqft_cume_dist", ignorenulls=True).over(w_recent).alias("first_known_running_sum_sqft_cume_dist"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.revenue").isNotNull()), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "running_count_sqft",
        "row_seq",
        "running_sum_sqft_cume_dist",
        "running_max_running_avg_sqft",
        "running_sum_running_avg_sqft",
        "running_avg_running_avg_sqft",
    )
    .orderBy(
        F.col("running_avg_running_avg_sqft").desc_nulls_last(),
        F.col("running_sum_sqft_cume_dist").desc(),
    )
)
