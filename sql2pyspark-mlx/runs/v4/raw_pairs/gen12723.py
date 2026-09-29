from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

with_gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("device").rlike("^(tablet|desktop)$"), "GROUP_A").when(F.col("device").rlike("^(mobile)$"), "EXTENDED").otherwise("GROUP_B").alias("device_zone"),
        F.col("max_account_balance"),
    )
    .orderBy(F.col("device_zone").desc())
)
