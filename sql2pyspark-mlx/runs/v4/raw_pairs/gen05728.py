from pyspark.sql import functions as F

enriched = (
    df_inventory
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.opened_date"), F.col("p.city"), F.col("p.sqft"))
)

result = (
    enriched
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .orderBy(F.col("avg_sqft").asc_nulls_first())
)
