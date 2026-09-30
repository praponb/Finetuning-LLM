from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first())
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

eligible = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < "2024-08-21"), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.cume_dist().over(w).alias("sqft_cume_dist"),
        F.lead("sqft", 2, 0).over(w2).alias("next_sqft"),
    )
)

result = (
    enriched
    .select("id", "store_name", "city", "opened_date", "sqft", "next_sqft")
    .orderBy(F.col("city").asc_nulls_last())
)
