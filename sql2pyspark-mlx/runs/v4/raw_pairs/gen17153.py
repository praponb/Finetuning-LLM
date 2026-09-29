from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("credit_limit").desc(), "id")
w_part = Window.partitionBy("city").orderBy("credit_limit_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy("credit_limit_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("last_known_credit_limit", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("city")
w8 = Window.partitionBy("city").orderBy("credit_limit_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter(F.col("city") == "New York")
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") < "2025-07-28"), "left_anti")
)

scored = (
    kept
    .withColumn("credit_limit_avg_5", F.avg("credit_limit").over(w))
    .withColumn("last_known_credit_limit", F.last("credit_limit", ignorenulls=True).over(w2))
    .withColumn("credit_limit_bucket", F.ntile(10).over(w3))
)

metrics = (
    scored
    .withColumn("last_known_credit_limit_bucket", F.last("credit_limit_bucket", ignorenulls=True).over(w_part))
    .withColumn("running_last_known_credit_limit", F.sum("last_known_credit_limit").over(w5))
)

result = (
    metrics
    .select(
        "*",
        F.avg("running_last_known_credit_limit").over(w6).alias("running_last_known_credit_limit_avg_2_prior"),
        F.max("last_known_credit_limit").over(w7).alias("city_max_last_known_credit_limit"),
        F.sum("last_known_credit_limit").over(w8).alias("running_last_known_credit_limit_2"),
    )
)
