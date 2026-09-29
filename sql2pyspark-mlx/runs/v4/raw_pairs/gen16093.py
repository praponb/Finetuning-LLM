from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date_plus_73d").asc_nulls_first(), "id")

cleaned = (
    df_stores
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-04-01")))
    .select("*", F.date_add("opened_date", 73).alias("opened_date_plus_73d"))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.opened_date_plus_73d")), "left_anti")
)

latest = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "opened_date_plus_73d",
        F.when(F.col("region").rlike("^(North|Central)$"), "CORE").otherwise("OTHER_ZONE").alias("region_zone"),
    )
)
