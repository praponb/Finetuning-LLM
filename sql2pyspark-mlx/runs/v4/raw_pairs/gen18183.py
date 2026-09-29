from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

with_last = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

result = (
    with_last
    .groupBy("region")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("city") == "London", F.col("quantity")).otherwise(0)).alias("london_quantity"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy("region")
)
