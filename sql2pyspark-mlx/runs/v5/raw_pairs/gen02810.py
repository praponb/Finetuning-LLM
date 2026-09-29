from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("plan_length", "id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

base = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-04-14").cast("date")) & (F.col("end_date") < "2025-02-01"))
    .select("*", F.length("plan").alias("plan_length"))
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

matched_keys = (
    ranked
    .select("status", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date").between("2023-06-01", "2025-02-01"))
        .select("status", "plan")
    )
)

verified = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("customer_id").isin([7, 30, 32, 40]))
    .withColumn("greatest_plan_length_monthly_fee", F.greatest("plan_length", "monthly_fee"))
)

result = (
    cleaned
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        "greatest_plan_length_monthly_fee",
        F.col("max_order_shipping_fee"),
    )
    .orderBy(F.col("greatest_plan_length_monthly_fee").asc_nulls_last(), "status")
)
