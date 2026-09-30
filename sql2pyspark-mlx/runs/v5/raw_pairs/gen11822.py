from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w_hist = Window.orderBy(F.col("days_end_date_to_start_date").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("days_end_date_to_start_date").desc(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("seq_no").desc_nulls_last(), "id")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-05-01").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("plan_prefix", F.substring("plan", 1, 4))
    .withColumn("days_end_date_to_start_date", F.datediff("start_date", "end_date"))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("e.*", F.col("b.name"))
)

metrics = (
    with_dims
    .filter(F.col("start_date").between("2024-01-19", "2024-10-11"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        "plan_prefix",
        "days_end_date_to_start_date",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.cume_dist().over(w_hist).alias("days_end_date_to_start_date_cume_dist"),
    )
)

metrics_subscriptions = (
    metrics
    .select(
        "id",
        "customer_id",
        "status",
        "monthly_fee_share",
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

deduped = (
    metrics_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy("customer_id")
    .pivot("status", ["ACTIVE", "CANCELLED", "PAUSED"])
    .agg(F.max("row_seq").alias("total"))
)
