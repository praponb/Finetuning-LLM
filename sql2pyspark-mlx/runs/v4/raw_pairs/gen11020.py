from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        F.explode("coupon_codes").alias("element"),
    )
)

matched_keys = (
    flattened
    .select("id", "customer_id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isin([19, 38]))
        .select("id", "customer_id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "created_at",
        "k",
        "element",
        F.when(F.col("element").rlike("^(VIP|BUNDLE)$"), "CORE").otherwise("GROUP_A").alias("element_zone"),
        F.col("min_account_balance"),
    )
    .orderBy(F.col("customer_id").asc_nulls_first())
)
