from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("city").orderBy("sqft", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w5 = Window.orderBy("city_length", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("city_length")
w7 = Window.orderBy(F.col("sqft_bucket").desc(), "id")

scored = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.ntile(10).over(w).alias("sqft_bucket"),
        F.avg("sqft").over(w2).alias("sqft_avg_6_prior"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

valid = (
    scored
    .filter(F.col("opened_date") > F.lit("2023-04-16").cast("date"))
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "sqft_bucket",
        F.when(F.col("sqft_bucket") >= 8, "High").when(F.col("sqft_bucket") >= 3, "Medium").otherwise("Low").alias("sqft_bucket_tier"),
    )
)

base = (
    valid
    .filter((F.col("opened_date") > F.lit("2023-07-30").cast("date")) & (F.year("opened_date") == 2023))
    .select("*", F.length("city").alias("city_length"))
)

with_last = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.sqft",
        "s.sqft_bucket",
        "s.sqft_bucket_tier",
        "s.city_length",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

result = (
    with_last
    .select(
        "id",
        "store_sale_units",
        F.avg("store_sale_units").over(w5).alias("store_sale_units_avg_6"),
        F.dense_rank().over(w6).alias("city_length_dense_rank"),
        F.ntile(3).over(w7).alias("sqft_bucket_bucket"),
    )
)
