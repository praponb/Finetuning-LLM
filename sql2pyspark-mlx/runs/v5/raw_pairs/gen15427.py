from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "attr_key",
        F.when(F.col("attr_key").rlike("^(gift|device)$"), "GROUP_B").when(F.col("attr_key").rlike("^(utm_source|weight_kg)$"), "GROUP_A").otherwise("CORE").alias("attr_key_zone"),
        F.col("sum_order_amount"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first(), "sum_order_amount")
)
