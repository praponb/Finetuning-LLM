from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w5 = Window.orderBy("sqft", "id").rowsBetween(-3, -1)
w6 = Window.orderBy("running_sum_sqft_scaled", "id")
w7 = Window.partitionBy("city").orderBy("store_sale_revenue", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy("city").orderBy("opened_date", "id")
w9 = Window.partitionBy("region").orderBy(F.col("running_avg_sqft_scaled").desc(), "id")
w10 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(-2, -1)

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("opened_date") > F.lit("2023-06-10").cast("date")))
    .withColumn("is_chiang_mai", F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))
    .withColumn("sqft_scaled", F.round((F.col("sqft") * 100), 2))
)

scored = (
    filtered
    .withColumn("running_max_sqft_scaled", F.max("sqft_scaled").over(w))
    .withColumn("running_avg_sqft_scaled", F.avg("sqft_scaled").over(w))
    .withColumn("running_sum_sqft_scaled", F.sum("sqft_scaled").over(w))
    .withColumn("region_stddev_samp_sqft_scaled", F.stddev_samp("sqft_scaled").over(w2))
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w4))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

enriched = (
    with_last
    .filter(F.col("running_avg_sqft_scaled") < 60)
    .select(
        "*",
        F.sum("running_avg_sqft_scaled").over(w2).alias("region_sum_running_avg_sqft_scaled"),
        F.avg("is_chiang_mai").over(w5).alias("is_chiang_mai_avg_3_prior"),
        F.ntile(4).over(w6).alias("running_sum_sqft_scaled_bucket"),
        F.avg("region_stddev_samp_sqft_scaled").over(w7).alias("region_stddev_samp_sqft_scaled_avg_3_prior"),
    )
)

scored_stores = (
    enriched
    .select(
        "*",
        F.row_number().over(w8).alias("row_seq"),
        F.ntile(3).over(w9).alias("running_avg_sqft_scaled_bucket"),
        F.avg("sqft").over(w10).alias("sqft_avg_2_prior"),
    )
)

result = (
    scored_stores
    .select(
        "id",
        "is_chiang_mai",
        "sqft_scaled",
        "running_avg_sqft_scaled",
        "region_stddev_samp_sqft_scaled",
        "rn",
        "store_sale_sale_date",
        "region_sum_running_avg_sqft_scaled",
        "is_chiang_mai_avg_3_prior",
    )
    .orderBy(F.col("sqft_scaled").desc_nulls_last(), "is_chiang_mai_avg_3_prior")
)
