from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.date_add("opened_date", 85).alias("opened_date_plus_85d"),
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.shipping_fee") > 27), "left_anti")
)

valid = (
    eligible
    .filter(F.col("opened_date_plus_85d").between("2024-04-01", "2025-07-12"))
    .select(
        "id",
        "store_name",
        "opened_date",
        "opened_date_plus_85d",
        "store_name_normalized",
        F.regexp_replace("store_name", " ", "_").alias("store_name_slug"),
        F.last_day("opened_date_plus_85d").alias("opened_date_plus_85d_month_end"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

remaining_keys = (
    with_last
    .select("id")
    .exceptAll(
        df_stores
        .filter(F.col("sqft").between(7574, 12558))
        .select("id")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    reconciled
    .groupBy(F.date_trunc("YEAR", "opened_date_plus_85d_month_end").alias("year_start"))
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_3"),
    )
)

result = (
    daily
    .select(
        "year_start",
        "quantity_quartiles",
        "quantity_quartiles_2",
        "quantity_quartiles_3",
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)
