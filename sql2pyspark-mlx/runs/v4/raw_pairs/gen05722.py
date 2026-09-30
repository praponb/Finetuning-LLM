from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id")
w3 = Window.partitionBy("browser", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

metrics = (
    df_sessions
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w_grp).alias("last_known_browser"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country"),
    )
)

result = (
    metrics
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "last_known_browser",
        "row_seq",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("device_zone"),
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("country").desc_nulls_last())
)
