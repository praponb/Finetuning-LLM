from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date_month_end", "id")
w3 = Window.orderBy("opened_date_month_end", "id")

base = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-03-09"))) & F.col("region").isNotNull())
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.concat("region", F.lit(" - "), "store_name").alias("region_store_name"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

filtered = (
    base
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-05-03")))
    .withColumn("region_group", F.when(F.col("region").isin(["East", "North"]), "Priority").otherwise("Other"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("OTHER")))
)

base_stores = (
    filtered
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("East"))))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("sqft_str", F.col("sqft").cast("string"))
)

not_flagged = (
    base_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.units") != 20), "left_anti")
)

not_flagged_stores = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.quantity") <= 113), "left_anti")
)

scored = (
    not_flagged_stores
    .filter(F.year("opened_date_month_end") == 2023)
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_3"),
        F.row_number().over(w2).alias("seq_no"),
        F.lead("sqft", 1).over(w3).alias("next_sqft"),
    )
)

filtered_stores = (
    scored
    .filter((F.col("opened_date") > F.lit("2023-04-10").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "region",
        "sqft",
        "region_clean",
        "region_group",
        "resolved_region",
        "opened_date_month_end",
        "sqft_str",
        "sqft_avg_3",
        "seq_no",
        F.floor("seq_no").alias("seq_no_whole"),
        F.greatest("sqft_avg_3", "next_sqft").alias("greatest_sqft_avg_3_next_sqft"),
    )
)

result = (
    filtered_stores
    .groupBy(
        "seq_no",
        F.when(F.col("greatest_sqft_avg_3_next_sqft") >= 81, "PREMIUM").otherwise("STANDARD").alias("greatest_sqft_avg_3_next_sqft_tier"),
    )
    .agg(
        F.avg("greatest_sqft_avg_3_next_sqft").alias("avg_greatest_sqft_avg_3_next_sqft"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
    )
    .orderBy(F.col("seq_no").desc_nulls_first())
)
