from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(F.date_trunc("year", "opened_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
    )
)
