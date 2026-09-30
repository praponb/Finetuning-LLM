from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-05-27").cast("date")) & F.col("start_date").between("2024-05-02", "2025-10-01"))
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        F.dayofweek("start_date").alias("start_date_dayofweek"),
    )
)

filtered = (
    valid
    .filter((F.col("end_date") > F.lit("2023-08-05").cast("date")) & F.col("monthly_fee").between(110, 120))
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        "start_date_dayofweek",
        F.ceil("start_date_dayofweek").alias("start_date_dayofweek_whole"),
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
    )
)

valid_subscriptions = (
    filtered
    .filter((F.col("end_date") > F.lit("2023-06-02").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("monthly_fee_rounded_int", F.col("monthly_fee_rounded").cast("int"))
)

joined = (
    valid_subscriptions
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"), F.col("b.name"))
)

result = (
    joined
    .groupBy("customer_id", "status")
    .pivot("city", ["London", "Singapore"])
    .agg(
        F.avg("monthly_fee_rounded_int").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
)
