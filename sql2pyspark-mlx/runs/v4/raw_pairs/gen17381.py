from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc(), "id", "attr_key")
w2 = Window.partitionBy("segment").orderBy("created_at_day", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("segment", "attr_key").orderBy(F.col("created_at_day").desc_nulls_last())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

enriched = (
    flattened
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.age"), F.col("b.segment"))
)

filtered = (
    enriched
    .filter(F.col("age") >= 58)
    .select(
        "id",
        "attr_key",
        "attr_value",
        "age",
        "segment",
        F.col("created_at").cast("date").alias("created_at_day"),
    )
)

enriched_carts = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.avg("age").over(w2).alias("age_avg_2"),
    )
)

deduped = (
    enriched_carts
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

stats = (
    deduped
    .groupBy(F.date_trunc("WEEK", "created_at_day").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("attr_key") == "utm_source", F.col("age")).otherwise(0)).alias("utm_source_age"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
    )
)

result = stats.select("week_start", "utm_source_age", "consumer_events")
