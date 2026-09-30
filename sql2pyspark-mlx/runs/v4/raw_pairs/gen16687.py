from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_credit").desc(), "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("txn_type", "merchant").orderBy("is_credit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2024-04-15 20:00:00").cast("timestamp"))
    .withColumn("is_credit", F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))
    .withColumn("txn_type_group", F.when(F.col("txn_type").isin(["REFUND", "CREDIT"]), "Group A").otherwise("Other"))
)

enriched = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(5).over(w2).alias("amount_bucket"),
        F.sum("is_credit").over(w3).alias("cumulative_is_credit"),
    )
)

matched_keys = (
    enriched
    .select("merchant")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_time") >= "2025-02-01")
        .select("merchant")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

wide = (
    kept_rows
    .groupBy("merchant")
    .pivot("txn_type", ["CREDIT", "REFUND"])
    .agg(F.max("is_credit"))
)

result = (
    wide
    .select(
        "merchant",
        "REFUND",
        F.when(F.col("merchant").rlike("^(Shell|Lazada)$"), "GROUP_B").otherwise("PRIORITY").alias("merchant_zone"),
    )
    .orderBy(F.col("merchant_zone").asc_nulls_last())
)
