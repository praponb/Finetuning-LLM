from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("starbucks_amount").desc())

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-10-01 06:00:00").cast("timestamp")) & (F.col("amount") < 3700))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("merchant_upper", F.upper("merchant"))
)

result = (
    base
    .groupBy(
        "account_id",
        F.when(F.col("amount_filled") >= 66, "LARGE").otherwise("SMALL").alias("amount_filled_tier"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
    )
    .filter(F.col("starbucks_amount") >= 0)
    .withColumn("starbucks_amount_rank", F.rank().over(w))
)
