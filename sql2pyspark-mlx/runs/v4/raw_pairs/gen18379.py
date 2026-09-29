from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy(F.col("shipping_fee").asc_nulls_last(), "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

deduped = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

matched = (
    deduped
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

cleaned = (
    matched
    .filter((F.col("order_date") > F.lit("2023-08-22").cast("date")) & F.col("customer_id").isNotNull() & F.col("promo_code").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        F.round((F.col("promo_id") * 1.1), 2).alias("promo_id_adjusted"),
        F.round((F.col("amount") * 1.07), 2).alias("amount_adjusted"),
        F.coalesce("promo_code", F.lit("UNKNOWN")).alias("promo_code_clean"),
    )
)

result = (
    cleaned
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "order_date",
        "shipping_fee",
        "promo_id",
        "promo_code",
        "amount_adjusted",
        F.when(F.col("promo_code").rlike("^(BUNDLE|VIP)$"), "GROUP_B").when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "EXTENDED").otherwise("CORE").alias("promo_code_zone"),
        F.col("min_inventory_quantity"),
    )
)
