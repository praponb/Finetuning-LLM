from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter((~F.col("region").eqNullSafe(F.lit("East"))) & (F.col("opened_date") > F.lit("2023-02-17").cast("date")))
    .select(
        "id",
        "store_name",
        "city",
        F.concat("city", F.lit(", "), "region").alias("city_region"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.city", "s.city_region", "s.region_clean", F.col("c.channel"))
)

remaining_keys = (
    latest_match
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("Ma%"))
        .select("id")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.status").isin(["SHIPPED", "COMPLETED"])), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "city",
        "city_region",
        "region_clean",
        "channel",
        F.when(F.col("city").rlike("^(London|Singapore)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "city_zone")
)
