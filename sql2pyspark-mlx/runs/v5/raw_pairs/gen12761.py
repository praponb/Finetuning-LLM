from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.bit_or("units").alias("units_bit_or"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("units_bit_and") > 1)
    .orderBy(F.col("units_bit_and").desc_nulls_last())
)
