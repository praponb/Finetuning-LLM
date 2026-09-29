from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("rating_tier").orderBy(F.col("th_count").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.when(F.col("rating") >= 4.3, "High").when(F.col("rating") >= 2.1, "Medium").otherwise("Low").alias("rating_tier"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("rating") >= 4.6, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
        "country",
    )
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.sum("rating").alias("total_rating"),
    )
    .withColumn("th_count_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("country").asc_nulls_last())
)
