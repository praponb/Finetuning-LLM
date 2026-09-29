from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("start_date_dayofweek", "id")
w2 = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("plan_length").desc())
w4 = Window.partitionBy("plan").orderBy(F.col("start_date").asc_nulls_first())
w5 = Window.partitionBy("status").orderBy(F.col("plan_count_start_date_dayofweek").desc())
w6 = Window.partitionBy("status").orderBy(F.col("plan_length").desc(), "id")
w7 = Window.partitionBy("plan").orderBy("plan_count_start_date_dayofweek", "id")
w8 = Window.partitionBy("plan", "status").orderBy("start_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("plan_count_start_date_dayofweek", "id").rowsBetween(-6, -1)
w10 = Window.orderBy("plan_length_rank", "id")

filtered = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-04-19").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        F.dayofweek("start_date").alias("start_date_dayofweek"),
        F.length("plan").alias("plan_length"),
    )
)

enriched = (
    filtered
    .withColumn("next_start_date_dayofweek", F.lead("start_date_dayofweek", 1).over(w))
    .withColumn("plan_count_start_date_dayofweek", F.count("start_date_dayofweek").over(w2))
    .withColumn("plan_length_rank", F.rank().over(w3))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest = (
    deduped
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "plan",
        "status",
        "start_date_dayofweek",
        "plan_length",
        "plan_count_start_date_dayofweek",
        "plan_length_rank",
    )
)

with_windows = (
    latest
    .filter(~F.col("plan").isin(["premium", "standard"]))
    .select(
        "id",
        "start_date_dayofweek",
        "plan_count_start_date_dayofweek",
        "plan_length_rank",
        F.max("plan_length").over(w2).alias("plan_max_plan_length"),
        F.row_number().over(w6).alias("event_seq"),
        F.ntile(10).over(w7).alias("plan_count_start_date_dayofweek_bucket"),
        F.max("plan_length_rank").over(w2).alias("plan_max_plan_length_rank"),
        F.sum("plan_count_start_date_dayofweek").over(w8).alias("cumulative_plan_count_start_date_dayofweek"),
    )
)

metrics = (
    with_windows
    .withColumn("plan_count_start_date_dayofweek_avg_6_prior", F.avg("plan_count_start_date_dayofweek").over(w9))
    .withColumn("seq_no", F.row_number().over(w10))
)

kept_keys = (
    metrics
    .select("id")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date").between("2024-04-01", "2025-01-29"))
        .select("id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "plan_count_start_date_dayofweek",
        "plan_length_rank",
        "plan_max_plan_length",
        "event_seq",
        "plan_count_start_date_dayofweek_bucket",
        "cumulative_plan_count_start_date_dayofweek",
    )
)
