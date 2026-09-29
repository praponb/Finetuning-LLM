from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("value"))

result = (
    lines
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "value",
        F.when(F.col("value").rlike("^(WELCOME5|VIP)$"), "OTHER_ZONE").when(F.col("value").rlike("^(SPRING10|BUNDLE)$"), "GROUP_A").otherwise("GROUP_B").alias("value_zone"),
        F.col("avg_account_balance"),
    )
)
