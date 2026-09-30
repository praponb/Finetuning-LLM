from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("discount").desc_nulls_first(), "id")

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-23").cast("date")) & F.col("channel").isNotNull() & F.col("store_id").isin([9, 17, 28, 39]))
    .withColumn("discount_whole", F.ceil("discount"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    latest
    .filter(F.col("order_date") > F.lit("2023-05-10").cast("date"))
    .select(
        "*",
        F.when(F.col("shipping_fee") >= 31, "High").when(F.col("shipping_fee") >= 20, "Medium").otherwise("Low").alias("shipping_fee_level"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

matched = (
    filtered
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "order_date",
        "status",
        "discount",
        "resolved_channel",
        "promo_id",
        "discount_pct",
        F.when(F.col("status").rlike("^(RETURNED|SHIPPED)$"), "EXTENDED").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("order_date").desc(), F.col("id").desc())
)
