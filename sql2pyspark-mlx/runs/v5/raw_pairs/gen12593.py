from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")

filtered = (
    df_stores
    .filter(F.col("opened_date") >= "2024-10-01")
    .withColumn("region_length", F.length("region"))
)

kept = (
    filtered
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.opened_date")), "left_semi")
)

deduped = (
    kept
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept_keys = (
    deduped
    .select("region", "id")
    .subtract(
        df_stores
        .filter(F.col("city") == "New York")
        .select("region", "id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("store_name", "city", "region", "region_length")
    .orderBy(F.col("region").desc_nulls_last())
)
