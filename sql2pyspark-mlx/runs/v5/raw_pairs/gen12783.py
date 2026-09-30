from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc_nulls_first(), "id")
w2 = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "account_id", "txn_time", "merchant")
)

enriched = (
    ranked
    .filter(F.col("txn_time") >= "2024-11-01")
    .select(
        "id",
        "merchant",
        F.first("merchant", ignorenulls=True).over(w2).alias("first_known_merchant"),
        F.first("merchant", ignorenulls=True).over(w3).alias("first_known_merchant_2"),
    )
)

filtered = (
    enriched
    .filter(F.col("merchant").isNotNull())
    .select("*", F.length("merchant").alias("merchant_length"))
)

cleaned = (
    filtered
    .filter(F.col("merchant").isin(["Amazon", "7-Eleven"]))
    .select(
        "*",
        F.length("merchant").alias("merchant_length_2"),
        F.coalesce("merchant_length", F.lit(0)).alias("merchant_length_filled"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

daily = (
    cleaned
    .groupBy(
        F.when(F.col("merchant_length_2") >= 80, "LARGE").otherwise("SMALL").alias("merchant_length_2_tier"),
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.percentile_approx("merchant_length_filled", 0.9).alias("merchant_length_filled_p90"),
    )
)

result = (
    daily
    .select("merchant_length_2_tier", "amazon_count", "merchant_length_filled_p90")
    .orderBy("merchant_length_filled_p90")
)
