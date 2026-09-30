from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date_period", "id")
w2 = Window.partitionBy("region").orderBy("opened_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("region").orderBy(F.col("new_group_flag").desc_nulls_first())
w_grp_2 = Window.orderBy("prev_opened_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stores
    .filter(F.col("city") == "London")
    .select("*", F.date_trunc("MONTH", "opened_date").alias("opened_date_period"))
)

prev_events = filtered.withColumn("prev_opened_date_period", F.lag("opened_date_period").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_opened_date_period").isNull() | (F.col("opened_date_period") > F.col("prev_opened_date_period") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

metrics = (
    sessionized
    .select(
        "*",
        F.percent_rank().over(w_grp).alias("new_group_flag_percent_rank"),
        F.sum("sqft").over(w_grp_2).alias("cumulative_sqft"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("MONTH", "prev_opened_date_period").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.min("prev_opened_date_period").alias("first_prev_opened_date_period"),
        F.min("opened_date_period").alias("started_at"),
    )
    .orderBy(F.col("east_count").desc_nulls_last())
)
