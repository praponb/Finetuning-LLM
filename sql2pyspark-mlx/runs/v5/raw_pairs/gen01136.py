from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")

filtered = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-09-26").cast("date"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.last_day("start_date").alias("start_date_month_end"),
    )
)

filtered_subscriptions = (
    filtered
    .filter(F.col("customer_id").isNotNull() & F.col("status").isin(["CANCELLED", "EXPIRED"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "start_date_month_end",
        F.when(F.col("monthly_fee") >= 246, "High").when(F.col("monthly_fee") >= 85, "Medium").otherwise("Low").alias("monthly_fee_level"),
        F.date_add("start_date", 34).alias("start_date_plus_34d"),
    )
)

joined = (
    filtered_subscriptions
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.start_date",
        "a.monthly_fee",
        "a.start_date_month_end",
        "a.monthly_fee_level",
        "a.start_date_plus_34d",
        F.col("p.name"),
        F.col("p.age"),
        F.col("p.credit_limit"),
    )
)

with_dims = (
    joined
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.age").alias("customer_age"))
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered_subscriptions_2 = (
    ranked
    .filter(F.col("customer_id").isNotNull())
    .withColumn("credit_limit_times_dedup_rank", (F.col("credit_limit") * F.col("dedup_rank")))
    .withColumn("monthly_fee_minus_age", (F.col("monthly_fee") - F.col("age")))
)

result = (
    filtered_subscriptions_2
    .select(
        "id",
        "customer_id",
        F.col("monthly_fee_minus_age").alias("MONTHLY_FEE_MINUS_AGE"),
        F.col("credit_limit_times_dedup_rank").alias("CREDIT_LIMIT_TIMES_DEDUP_RANK_VAL"),
        "credit_limit",
    )
    .unpivot(
        ids=["id", "customer_id"],
        values=["MONTHLY_FEE_MINUS_AGE", "CREDIT_LIMIT_TIMES_DEDUP_RANK_VAL", "credit_limit"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
