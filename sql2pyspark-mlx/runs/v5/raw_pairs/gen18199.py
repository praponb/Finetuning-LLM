from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("start_date").asc_nulls_first(), "id")
w_running = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-07-01").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.dayofweek("start_date").alias("start_date_dayofweek"),
        F.length("plan").alias("plan_length"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.avg("plan_length").over(w_running).alias("running_avg_plan_length"),
        F.min("plan_length").over(w_running).alias("running_min_plan_length"),
        F.sum("plan_length").over(w_running).alias("running_sum_plan_length"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.monthly_fee",
        "a.status",
        "a.start_date_dayofweek",
        "a.plan_length",
        "a.running_min_plan_length",
        F.col("c.city"),
        F.col("c.country"),
    )
)

result = (
    with_dims
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan_length",
        "running_min_plan_length",
        "city",
        "country",
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("customer_id").asc_nulls_last())
)
