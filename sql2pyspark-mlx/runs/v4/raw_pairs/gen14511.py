from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy(F.col("end_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy(F.col("monthly_fee").asc_nulls_first())
w5 = Window.orderBy(F.col("end_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("plan")

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-09-23").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.date_trunc("year", "end_date").alias("end_date_period"),
        F.add_months("end_date", 1).alias("end_date_plus_months"),
    )
)

kept_keys = (
    base
    .select("customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date").between("2024-04-01", "2024-09-01"))
        .select("customer_id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("status_sum_monthly_fee", F.sum("monthly_fee").over(w))
    .withColumn("monthly_fee_rank", F.rank().over(w2))
    .withColumn("monthly_fee_avg_7d", F.avg("monthly_fee").over(w3))
)

with_dims = (
    scored
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.name"), F.col("d.city"))
)

result = (
    with_dims
    .select(
        "*",
        F.rank().over(w4).alias("monthly_fee_rank_2"),
        F.sum("status_sum_monthly_fee").over(w5).alias("status_sum_monthly_fee_sum_7d"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w6)), 4).alias("monthly_fee_share"),
    )
)
