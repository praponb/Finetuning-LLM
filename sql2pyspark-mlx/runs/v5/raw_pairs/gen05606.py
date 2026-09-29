from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        F.when(F.col("txn_type").isin(["REFUND", "CREDIT"]), "Priority").otherwise("Other").alias("txn_type_group"),
    )
)

flagged = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "txn_type_group",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessions_transactions
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .withColumn("island_id_tier", F.when(F.col("island_id") >= 6, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["year_start", "island_id_tier"], ["year_start"], []],
        "year_start",
        "island_id_tier",
    )
    .agg(
        F.sum("island_id").alias("total_island_id"),
        F.count("*").alias("cnt"),
        F.grouping("year_start").alias("grouping_id"),
    )
    .orderBy(F.col("island_id_tier").asc_nulls_last())
)
