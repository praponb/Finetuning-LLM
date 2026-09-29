from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy(F.col("row_num").desc())
w3 = Window.orderBy(F.col("sqft").desc_nulls_first())
w4 = Window.partitionBy("region").orderBy("opened_date_plus_months", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("region") == "North"))
    .select("*", F.upper("region").alias("region_upper"))
)

cleaned = (
    valid
    .filter((F.col("opened_date") > F.lit("2023-06-06").cast("date")) & ~F.col("region").isin(["Central", "East"]))
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 12))
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped_stores = (
    deduped
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "region", "opened_date", "sqft", "opened_date_plus_months", "row_num")
)

metrics = (
    deduped_stores
    .filter(F.col("row_num") >= 1)
    .withColumn("sqft_cume_dist", F.cume_dist().over(w3))
    .withColumn("sqft_avg_3", F.avg("sqft").over(w4))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("sqft_avg_3") >= 13966, "HIGH").otherwise("LOW").alias("sqft_avg_3_tier"),
        F.date_trunc("QUARTER", "opened_date_plus_months").alias("quarter_start"),
    )
    .agg(
        F.avg(F.when(F.col("region") == "North", F.col("sqft_cume_dist"))).alias("north_avg_sqft_cume_dist"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.avg(F.when(F.col("region") == "West", F.col("sqft"))).alias("west_avg_sqft"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft_avg_3")).otherwise(0)).alias("east_sqft_avg_3"),
    )
    .orderBy(F.col("quarter_start").desc())
)
