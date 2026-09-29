from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("status")
w3 = Window.orderBy("balance", "id")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("discount").alias("max_order_discount"))

metrics = (
    df_accounts
    .withColumn("balance_avg_2_prior", F.avg("balance").over(w))
    .withColumn("status_avg_balance", F.avg("balance").over(w2))
    .withColumn("balance_bucket", F.ntile(10).over(w3))
)

result = (
    metrics
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        "balance_avg_2_prior",
        "status_avg_balance",
        F.when(F.col("account_type").rlike("^(LOAN|CHECKING)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("account_type_zone"),
        F.col("max_order_discount"),
    )
    .orderBy(F.col("max_order_discount").desc())
)
