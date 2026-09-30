from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("method_upper", F.upper("method"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("N/A")))
)

metrics = (
    base
    .select(
        "*",
        F.first("amount", ignorenulls=True).over(w_part).alias("first_known_amount"),
        F.last("method", ignorenulls=True).over(w2).alias("last_known_method"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

filtered = (
    metrics
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.col("first_known_amount").cast("int").alias("first_known_amount_int"),
    )
)

result = (
    filtered
    .groupBy(F.date_trunc("quarter", "paid_at").alias("quarter_start"), "status")
    .agg(
        F.count_if(F.col("method") == "promptpay").alias("promptpay_events"),
        F.sum(F.when(F.col("method") == "card", F.col("amount_share")).otherwise(0)).alias("card_amount_share"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("first_known_amount_int")).otherwise(0)).alias("failed_first_known_amount_int"),
    )
    .orderBy(F.col("failed_first_known_amount_int").desc())
)
