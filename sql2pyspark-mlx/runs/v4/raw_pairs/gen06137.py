from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date_plus_months").desc_nulls_last())
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "status").orderBy("rn", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id", "status").orderBy(F.col("start_date_plus_months").desc(), "id")
w6 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2024-04-05").cast("date"))
    .select(
        "*",
        F.when(F.col("status").isin(["CANCELLED", "ACTIVE"]), "Group A").otherwise("Other").alias("status_group"),
        F.add_months("start_date", 3).alias("start_date_plus_months"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w2))
    .withColumn("monthly_fee_avg_6_prior", F.avg("monthly_fee").over(w3))
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("cumulative_monthly_fee_2", F.sum("monthly_fee").over(w6))
)

result = (
    scored
    .select("id", "plan", "monthly_fee", "last_known_monthly_fee")
    .unpivot(
        ["id", "plan"],
        ["monthly_fee", "last_known_monthly_fee"],
        "metric_name",
        "value",
    )
)
