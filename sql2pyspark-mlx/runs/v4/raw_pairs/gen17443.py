from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("store_name_length").desc(), "id")

filtered = (
    df_stores
    .filter(F.year("opened_date") == 2023)
    .select("id", "store_name", "region", F.initcap(F.trim("city")).alias("city_title"))
)

cleaned = (
    filtered
    .filter(F.col("region").isNotNull())
    .select("*", F.length("store_name").alias("store_name_length"))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= "2023-03-18"), "left_semi")
)

deduped = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select("id", "region", "city_title", "store_name_length", "dedup_rank")
    .orderBy(F.col("region").desc_nulls_last())
)
