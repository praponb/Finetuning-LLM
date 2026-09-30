from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Tokyo", "Chiang Mai"])
    .agg(F.max("rating").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "country",
        F.col("Tokyo_value_sum").alias("tokyo_value_sum"),
        F.col("Tokyo_cnt").alias("tokyo_cnt"),
        F.col("Chiang Mai_value_sum").alias("chiang_mai_value_sum"),
        F.col("Chiang Mai_cnt").alias("chiang_mai_cnt"),
    )
)
