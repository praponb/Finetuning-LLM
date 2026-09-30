from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy(F.col("running_age").asc_nulls_first(), "id")
w_running = Window.partitionBy("city").orderBy("running_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_customers
    .filter(F.col("country") == "US")
    .select(
        "*",
        F.last("city", ignorenulls=True).over(w).alias("last_known_city"),
        F.first("city", ignorenulls=True).over(w2).alias("first_known_city"),
        F.sum("age").over(w3).alias("running_age"),
        F.first("segment", ignorenulls=True).over(w4).alias("first_known_segment"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") >= F.col("s.signup_date")), "left_anti")
)

deduped = (
    kept
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.max("running_age").over(w_running).alias("running_max_running_age"),
        F.avg("running_age").over(w_running).alias("running_avg_running_age"),
        F.min("running_age").over(w_running).alias("running_min_running_age"),
        F.count("running_age").over(w_running).alias("running_count_running_age"),
        F.avg("age").over(w7).alias("age_avg_3"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "email",
        "country",
        "city",
        "age",
        "first_known_segment",
        "latest_rank",
        "running_max_running_age",
        "running_avg_running_age",
    )
    .orderBy("running_avg_running_age", F.col("age").desc())
)
