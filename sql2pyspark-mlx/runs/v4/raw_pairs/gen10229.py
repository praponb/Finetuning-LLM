from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date_period", "id")
w2 = Window.partitionBy("plan").orderBy("end_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("end_date_plus_31d").desc_nulls_first(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("end_date_period").desc_nulls_first(), "id")

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-05-24").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.date_trunc("month", "end_date").alias("end_date_period"),
        F.date_add("end_date", 31).alias("end_date_plus_31d"),
    )
)

flagged = (
    base
    .withColumn("is_new_session", F.when(F.col("end_date_period") - F.lag("end_date_period").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_subscriptions = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

deduped = (
    sessions_subscriptions
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

deduped_subscriptions = (
    deduped
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped_subscriptions
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.age"), F.col("p.country"))
)

result = (
    enriched
    .groupBy(F.date_trunc("MONTH", "start_date").alias("activity_month"))
    .agg(
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .orderBy(F.col("num_status").asc_nulls_first())
)
