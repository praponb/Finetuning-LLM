from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("units_bit_or").desc())

cleaned = (
    df_store_sales
    .filter(F.col("units").isNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.date_trunc("YEAR", "sale_date").alias("sale_date_period"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("YEAR", "sale_date_period").alias("year_start"),
        F.when(F.col("units") >= 37, "HIGH").otherwise("LOW").alias("units_tier"),
    )
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.min("sale_date_period").alias("first_seen"),
        F.max("sale_date_period").alias("last_seen"),
    )
    .withColumn("units_bit_or_rank", F.rank().over(w))
    .orderBy(F.col("units_tier").desc())
)
