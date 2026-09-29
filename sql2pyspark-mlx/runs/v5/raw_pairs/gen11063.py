from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .cube(
        F.when(F.col("is_new_session") >= 1, "HIGH").otherwise("LOW").alias("is_new_session_tier"),
    )
    .agg(
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("txn_type") == "CREDIT", 1)), F.lit(0)), 2).alias("session_id_per_credit"),
    )
    .orderBy(F.col("is_new_session_tier").desc())
)
