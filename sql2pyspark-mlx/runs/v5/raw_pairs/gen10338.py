from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("plan").orderBy("monthly_fee_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("monthly_fee_share").asc_nulls_last())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-06-12").cast("date")) & F.col("end_date").between("2023-10-01", "2024-09-01"))
    .withColumn("plan_status_key", F.concat_ws("-", "plan", "status"))
)

with_dims = (
    valid
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("e.*", F.col("c.city"))
)

metrics = (
    with_dims
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "plan_status_key",
        F.lead("monthly_fee", 2).over(w).alias("next_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.last("next_monthly_fee", ignorenulls=True).over(w3).alias("last_known_next_monthly_fee"),
        F.rank().over(w4).alias("monthly_fee_share_rank"),
    )
)

result = (
    scored
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        "plan_status_key",
        "monthly_fee_share",
        "monthly_fee_share_rank",
        F.when(F.col("status").rlike("^(ACTIVE|CANCELLED)$"), "EXTENDED").when(F.col("status").rlike("^(PAUSED|EXPIRED)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
        F.col("min_order_amount"),
    )
    .orderBy(F.col("end_date").desc_nulls_last())
)
