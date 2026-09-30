from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc_nulls_last())

base = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-02-13").cast("date")) & (F.col("sqft") != 13582))
    .select(
        "*",
        F.when(F.col("sqft") >= 15464, "High").when(F.col("sqft") >= 4743, "Medium").otherwise("Low").alias("sqft_band"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    ranked
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("%Downtow%"))
        .select("id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([1, 5])), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "sqft_band",
        F.when(F.col("store_name").rlike("^(Old Town|Airport Kiosk)$"), "PRIORITY").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "EXTENDED").otherwise("GROUP_B").alias("store_name_zone"),
    )
    .orderBy(F.col("sqft").desc(), "region")
)
