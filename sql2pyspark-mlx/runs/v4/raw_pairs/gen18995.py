from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.orderBy("opened_date", "id")

enriched = (
    df_stores
    .select(
        "*",
        F.dense_rank().over(w).alias("sqft_dense_rank"),
        F.row_number().over(w2).alias("seq_no"),
        F.lead("sqft", 1).over(w3).alias("next_sqft"),
    )
)

result = (
    enriched
    .withColumn("activity_month", F.date_trunc("month", "opened_date"))
    .groupingSets(
        [["activity_month", "city"], ["activity_month"], []],
        "activity_month",
        "city",
    )
    .agg(
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft")).otherwise(0)).alias("singapore_sqft"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
