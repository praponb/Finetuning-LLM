from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("last7_max_amount").desc(), "id")

base = (
    df_transactions
    .filter(F.col("amount") <= 2600)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.concat("txn_type", F.lit(", "), "merchant").alias("txn_type_merchant"),
    )
)

scored = (
    base
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged",
        "txn_type_merchant",
        F.max("amount").over(w_running).alias("last7_max_amount"),
        F.sum("amount").over(w_running).alias("last7_sum_amount"),
        F.avg("amount").over(w_running).alias("last7_avg_amount"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "merchant", "txn_type_merchant")
)

cleaned = (
    ranked
    .filter(F.col("merchant").isNotNull() & ~F.col("merchant").isin(["Grab", "Starbucks"]))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("unassigned")))
    .withColumn("merchant_filled_2", F.coalesce("merchant", F.lit("OTHER")))
)

base_transactions = (
    cleaned
    .filter(~F.col("merchant").eqNullSafe(F.lit("Starbucks")))
    .select(
        "id",
        "merchant",
        "txn_type_merchant",
        F.lower("merchant").alias("merchant_lower"),
    )
)

cleaned_transactions = (
    base_transactions
    .filter(F.col("merchant").isin(["Starbucks", "Shell"]))
    .select("*", F.trim("merchant").alias("clean_merchant"))
)

valid = (
    cleaned_transactions
    .filter(F.col("merchant").isin(["Starbucks", "Lazada", "Amazon"]))
    .select(
        "id",
        "merchant",
        "txn_type_merchant",
        "merchant_lower",
        "clean_merchant",
        F.trim("merchant").alias("clean_merchant_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "merchant",
        "txn_type_merchant",
        "merchant_lower",
        "clean_merchant",
        "clean_merchant_2",
        F.when(F.col("merchant").rlike("^(Lazada|Amazon)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("merchant_zone"),
    )
    .orderBy("merchant_zone")
)
