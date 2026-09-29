from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("country", "customer_id").orderBy(F.col("started_at").desc(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

metrics = (
    df_sessions
    .withColumn("seq_no", F.row_number().over(w_grp))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

result = (
    metrics
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "seq_no",
        "event_seq",
        "row_seq",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "OTHER_ZONE").when(F.col("device").rlike("^(desktop)$"), "CORE").otherwise("GROUP_B").alias("device_zone"),
        F.col("sum_account_balance"),
    )
)
