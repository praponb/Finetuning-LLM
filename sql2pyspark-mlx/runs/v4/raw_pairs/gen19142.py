from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("amount", "id")
w3 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("amount").asc_nulls_first())
w4 = Window.partitionBy("txn_type").orderBy("amount", "id")

scored = (
    df_transactions
    .select(
        "*",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
        F.dense_rank().over(w3).alias("amount_dense_rank"),
        F.last("amount", ignorenulls=True).over(w).alias("last_known_amount"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

result = (
    scored
    .withColumn("quarter_start", F.date_trunc("QUARTER", "txn_time"))
    .withColumn("last_known_amount_tier", F.when(F.col("last_known_amount") >= 4000, "LARGE").otherwise("SMALL"))
    .rollup("quarter_start", "last_known_amount_tier")
    .agg(
        F.round(F.sum("next_amount") / F.nullif(F.count(F.when(F.col("merchant") == "Starbucks", 1)), F.lit(0)), 2).alias("next_amount_per_starbucks"),
        F.percentile_approx("first_known_amount", 0.9).alias("first_known_amount_p90"),
        F.grouping_id().alias("gid"),
    )
)
