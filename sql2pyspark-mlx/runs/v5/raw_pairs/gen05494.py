from pyspark.sql import functions as F

joined = (
    df_inventory
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.opened_date"), F.col("p.store_name"), F.col("p.sqft"))
)

result = (
    joined
    .withColumn("year_start", F.date_trunc("year", "last_restock"))
    .groupingSets(
        [["product_id", "year_start"], ["product_id"]],
        "product_id",
        "year_start",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("row_count_2") > 1)
    .orderBy(F.col("year_start").desc())
)
