from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("region").orderBy("opened_date", "id")
w_hist = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("region", "city").orderBy(F.col("event_seq").desc(), "id")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-11-08"))) & F.col("opened_date").between("2023-03-01", "2025-08-01"))
    .select(
        "*",
        F.when(F.col("sqft") >= 13104, "High").when(F.col("sqft") >= 4855, "Medium").otherwise("Low").alias("sqft_band"),
        F.when(F.col("city").isin(["Berlin", "London"]), "Tier 1").otherwise("Other").alias("city_group"),
    )
)

enriched = (
    base
    .withColumn("prev_sqft", F.lag("sqft", 2).over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_hist))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

scored = (
    with_latest
    .select(
        "*",
        F.ntile(4).over(w4).alias("event_seq_bucket"),
        F.sum("prev_sqft").over(w5).alias("city_sum_prev_sqft"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.last_restock")), "left_semi")
)

remaining_keys = (
    eligible
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("region").isin(["Central", "East"]))
        .select("id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft_band",
        "city_group",
        "prev_sqft",
        "quantity",
        "last_restock",
    )
    .orderBy(F.col("prev_sqft").asc_nulls_first())
)
