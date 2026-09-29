from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region")
w_hist = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("sqft", "id")

scored = (
    df_stores
    .filter(F.col("city").isin(["Singapore", "New York", "Bangkok"]))
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w).alias("city_sum_sqft"),
        F.last("region", ignorenulls=True).over(w_hist).alias("last_known_region"),
        F.lag("sqft", 1).over(w3).alias("prev_sqft"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("sqft") >= 14211, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count_2"),
    )
    .orderBy(F.col("row_count_2").desc_nulls_first())
)
