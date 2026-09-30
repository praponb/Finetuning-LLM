from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_flagged_flag", "id")
w2 = Window.partitionBy("account_id").orderBy("is_flagged_flag", "id")
w3 = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("account_id").orderBy("is_flagged_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Amazon"))))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        F.substring("txn_type", 1, 2).alias("txn_type_prefix"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

result = (
    valid
    .select(
        "*",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.lead("amount", 1, 0).over(w2).alias("next_amount_2"),
        F.row_number().over(w3).alias("seq_no"),
        F.last("amount", ignorenulls=True).over(w4).alias("last_known_amount"),
        F.ntile(5).over(w2).alias("is_flagged_flag_bucket"),
    )
)
