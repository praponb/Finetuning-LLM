from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "DE"])
    .agg(F.sum("rating").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "city",
        F.col("SG_amount").alias("sg_amount"),
        F.col("SG_cnt").alias("sg_cnt"),
        F.col("DE_amount").alias("de_amount"),
        F.col("DE_cnt").alias("de_cnt"),
    )
)
