from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

cleaned = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-06-20").cast("date"))
    .select(
        "id",
        "store_id",
        "channel",
        "discount",
        F.add_months("order_date", 1).alias("order_date_plus_months"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .filter(F.col("d.store_name").like("R%"))
    .select("s.id", "s.store_id", "s.channel_clean", F.col("d.city"), F.col("d.region"))
)

valid = (
    with_dims
    .filter(F.col("region") == "East")
    .withColumn("city_prefix", F.substring("city", 1, 4))
    .withColumn("resolved_city", F.coalesce("city", F.lit("OTHER")))
)

with_latest = (
    valid
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.city_prefix",
        F.col("c.status").alias("payment_status"),
        F.col("c.method").alias("payment_method"),
    )
)

base = (
    with_latest
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "city",
        "city_prefix",
        "payment_status",
        "payment_method",
        F.when(F.col("payment_status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
    )
)

matched_keys = (
    base
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("store_id").isNotNull())
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "city",
        "city_prefix",
        "is_success",
        F.when(F.col("city").rlike("^(Berlin|Singapore)$"), "CORE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("city").desc_nulls_first(), "city_zone")
)
