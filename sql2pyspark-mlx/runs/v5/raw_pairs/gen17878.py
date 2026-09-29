from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").asc_nulls_last())
w_running = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("customer_id").orderBy("end_date", "id")

ranked = (
    df_subscriptions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .filter(F.month("start_date").isin([1, 2, 4]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "row_num",
        F.ntile(10).over(w_running).alias("monthly_fee_bucket"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.status",
        F.col("p.country"),
        F.col("p.city"),
    )
)

base = (
    with_dims
    .filter(F.col("country").isNotNull() & (F.col("status") == "EXPIRED"))
    .select(
        "id",
        "customer_id",
        "status",
        "city",
        F.when(F.col("city").isin(["Bangkok", "Singapore"]), "Group A").otherwise("Other").alias("city_group"),
        F.upper("status").alias("status_upper"),
    )
)

enriched_subscriptions = (
    base
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.segment"), F.col("d.country"), F.col("d.credit_limit"))
)

result = (
    enriched_subscriptions
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 16100, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["city", "credit_limit_tier"], ["city"], []],
        "city",
        "credit_limit_tier",
    )
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
    )
    .filter(F.col("max_credit_limit") > 0)
    .orderBy(F.col("city").desc_nulls_first())
)
