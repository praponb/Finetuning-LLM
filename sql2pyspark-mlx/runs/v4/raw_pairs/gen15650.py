from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first())
w_grp = Window.partitionBy("region", "city")
w4 = Window.partitionBy("city").orderBy("opened_date", "id")
w5 = Window.orderBy(F.col("sqft").asc_nulls_last())

scored = (
    df_stores
    .select(
        "*",
        F.last("sqft", ignorenulls=True).over(w).alias("last_known_sqft"),
        F.dense_rank().over(w_ordered).alias("sqft_dense_rank"),
        F.count("sqft").over(w_grp).alias("region_count_sqft"),
        F.lag("sqft", 2, 0).over(w4).alias("prev_sqft"),
        F.cume_dist().over(w5).alias("sqft_cume_dist"),
    )
)

result = (
    scored
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .groupingSets([["city", "quarter_start"], ["city"]], "city", "quarter_start")
    .agg(
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.round(F.sum("sqft_cume_dist") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("sqft_cume_dist_per_bangkok"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
