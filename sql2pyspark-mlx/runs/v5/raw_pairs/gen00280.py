from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy(F.col("is_active_flag").desc_nulls_last(), "id")
w2 = Window.partitionBy("segment")
w_running = Window.partitionBy("segment", "city").orderBy("is_active_flag", "id")
w4 = Window.partitionBy("city").orderBy("is_active_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment", "city")
w6 = Window.partitionBy("city").orderBy("first_known_is_active_flag", "id").rowsBetween(-5, -1)
w7 = Window.orderBy("first_known_is_active_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("is_active_flag", "id")
w9 = Window.orderBy("is_active_flag", "id").rowsBetween(-5, -1)

base = (
    df_customers
    .filter(F.col("name").like("Jac%"))
    .select(
        "id",
        "email",
        "city",
        "segment",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_filled"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .withColumn("segment_count_is_active_flag", F.count("is_active_flag").over(w2))
    .withColumn("next_is_active_flag", F.lead("is_active_flag", 1).over(w_running))
    .withColumn("first_known_is_active_flag", F.first("is_active_flag", ignorenulls=True).over(w4))
)

enriched = (
    metrics
    .select(
        "id",
        "email",
        "is_active_flag",
        "next_is_active_flag",
        "first_known_is_active_flag",
        F.avg("first_known_is_active_flag").over(w5).alias("segment_avg_first_known_is_active_flag"),
        F.avg("is_active_flag").over(w6).alias("is_active_flag_avg_5_prior"),
        F.sum("next_is_active_flag").over(w7).alias("cumulative_next_is_active_flag"),
        F.ntile(3).over(w8).alias("is_active_flag_bucket"),
        F.avg("is_active_flag").over(w9).alias("is_active_flag_avg_5_prior_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "email",
        "is_active_flag",
        "first_known_is_active_flag",
        "segment_avg_first_known_is_active_flag",
        "is_active_flag_avg_5_prior_2",
    )
    .orderBy(F.col("is_active_flag").desc())
)
