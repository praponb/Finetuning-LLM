from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("activity_month").orderBy(F.col("is_new_session_per_add_to_cart").desc())

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "event_time").alias("activity_month"), "session_no")
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.round(F.sum("is_new_session") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("is_new_session_per_add_to_cart"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("is_new_session")).otherwise(0)).alias("checkout_is_new_session"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_rows"),
    )
    .withColumn("is_new_session_per_add_to_cart_dense_rank", F.dense_rank().over(w3))
    .orderBy("add_to_cart_rows")
)
