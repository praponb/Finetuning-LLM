from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id")
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_rounded").asc_nulls_last())
w3 = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("monthly_fee_rounded", "id")
w5 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())
w6 = Window.partitionBy("customer_id").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id", "status").orderBy("cumulative_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("monthly_fee_rounded", "id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2024-03-16").cast("date")) & F.col("customer_id").isNotNull() & (F.col("plan") == "basic"))
    .withColumn("monthly_fee_rounded", F.round("monthly_fee", 1))
)

metrics = (
    valid
    .withColumn("next_monthly_fee_rounded", F.lead("monthly_fee_rounded", 1).over(w))
    .withColumn("monthly_fee_rounded_dense_rank", F.dense_rank().over(w2))
    .withColumn("last_known_monthly_fee_rounded", F.last("monthly_fee_rounded", ignorenulls=True).over(w3))
)

enriched = (
    metrics
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.credit_limit"), F.col("d.segment"), F.col("d.email"))
)

scored = (
    enriched
    .withColumn("monthly_fee_rounded_bucket", F.ntile(4).over(w4))
    .withColumn("monthly_fee_rank", F.rank().over(w5))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w6))
)

enriched_subscriptions = (
    scored
    .select(
        "*",
        F.last("monthly_fee_rounded_dense_rank", ignorenulls=True).over(w7).alias("last_known_monthly_fee_rounded_dense_rank"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

result = (
    enriched_subscriptions
    .select(
        "id",
        "segment",
        "last_known_monthly_fee_rounded",
        F.col("credit_limit").alias("CREDIT_LIMIT_VAL"),
    )
    .unpivot(
        ["id", "segment"],
        ["last_known_monthly_fee_rounded", "CREDIT_LIMIT_VAL"],
        "measure",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
