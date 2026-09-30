from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date")

deduped = (
    df_stores
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.opened_date")), "left_anti")
)

kept_keys = (
    kept
    .select("region", "city")
    .subtract(
        df_stores
        .filter(F.col("sqft").isNotNull())
        .select("region", "city")
    )
)

verified = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "row_num",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Downtown Express)$"), "EXTENDED").when(F.col("store_name").rlike("^(Siam Flagship|Mall Central)$"), "PRIORITY").otherwise("GROUP_A").alias("store_name_zone"),
    )
    .orderBy(F.col("store_name_zone").desc())
)
