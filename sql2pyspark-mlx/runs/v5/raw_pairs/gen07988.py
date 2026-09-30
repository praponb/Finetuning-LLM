from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy(F.col("order_amount").desc(), "id")

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.col("sqft").cast("string").alias("sqft_str"),
        F.concat_ws("-", "store_name", "region").alias("store_name_region_key"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_latest = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"), F.col("c.amount").alias("order_amount"))
)

filtered = (
    with_latest
    .filter((F.col("opened_date") >= F.lit("2023-09-03").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft_str",
        "store_name_region_key",
        "order_amount",
        F.concat("store_name", F.lit(" "), "city").alias("store_name_city"),
        F.upper("city").alias("city_upper"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

remaining_keys = (
    deduped
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("%Riverside%"))
        .select("id", "city")
    )
)

verified = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "city",
        "sqft_str",
        "store_name_region_key",
        "store_name_city",
        "city_upper",
        F.when(F.col("city").rlike("^(Singapore|Chiang Mai)$"), "GROUP_B").otherwise("GROUP_A").alias("city_zone"),
    )
)
