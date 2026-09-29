from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan").orderBy(F.col("start_date_plus_months").desc(), "id")
w_hist_2 = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")
w3 = Window.partitionBy("status", "plan").orderBy(F.col("start_date").asc_nulls_first(), "id")
w_grp = Window.partitionBy("plan").orderBy("start_date_plus_months", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("plan", "status").orderBy("seq_no_2", "id")

valid = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-03-14").cast("date"))
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        F.add_months("start_date", 1).alias("start_date_plus_months"),
    )
)

metrics = (
    valid
    .filter(F.col("start_date") >= "2023-10-01")
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("seq_no_2", F.row_number().over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist_2))
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    ranked
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE", "CANCELLED"]))
    .select(
        "*",
        F.date_format("start_date", "yyyy-MM").alias("start_date_label"),
        (F.col("seq_no_2") * F.col("seq_no")).alias("seq_no_2_times_seq_no"),
    )
)

metrics_subscriptions = (
    filtered
    .withColumn("seq_no_avg_6_prior", F.avg("seq_no").over(w_grp))
    .withColumn("seq_no_3", F.row_number().over(w5))
)

result = (
    metrics_subscriptions
    .rollup("seq_no")
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("event_seq")).otherwise(0)).alias("enterprise_event_seq"),
    )
    .filter(F.col("enterprise_rows") > 1)
)
