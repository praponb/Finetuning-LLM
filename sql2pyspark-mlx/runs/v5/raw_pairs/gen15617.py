from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter((F.col("city") == "Bangkok") & (F.col("sqft") > 4796) & (F.col("sqft") == 13462))
)

agg = (
    valid_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 13588, "high").when(F.col("sqft") >= 7860, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sort_array(F.collect_set("region")).alias("region_list"))
)

result = agg.orderBy(F.col("city").desc())
