from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

with_last = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.sqft",
        F.col("c.quantity"),
    )
)

result = (
    with_last
    .select(
        "id",
        "city",
        "region",
        "quantity",
        F.when(F.col("region").rlike("^(West|North)$"), "EXTENDED").when(F.col("region").rlike("^(South|East)$"), "GROUP_A").otherwise("PRIORITY").alias("region_zone"),
    )
)
