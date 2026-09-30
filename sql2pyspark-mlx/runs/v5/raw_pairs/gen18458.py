from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("started_at").desc(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

scored = df_sessions.withColumn("row_seq", F.row_number().over(w))

result = (
    scored
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "row_seq",
        F.when(F.col("browser").rlike("^(Safari|Chrome)$"), "OTHER_ZONE").when(F.col("browser").rlike("^(Firefox|Edge)$"), "EXTENDED").otherwise("GROUP_A").alias("browser_zone"),
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("id").desc())
)
