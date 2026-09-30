from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("quantity_bit_and").desc())

with_dims = (
    df_inventory
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.region"), F.col("d.opened_date"), F.col("d.store_name"))
)

result = (
    with_dims
    .groupBy("region", F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .filter(F.col("quantity_bit_and") < 1)
    .withColumn("quantity_bit_and_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("region").desc_nulls_first())
)
