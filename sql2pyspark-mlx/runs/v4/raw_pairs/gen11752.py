from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("region")
w5 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("first_known_sqft", "id")
w7 = Window.orderBy("region_max_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city").orderBy(F.col("first_known_sqft").desc_nulls_last(), "id")

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-05-25").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.upper("region").alias("region_upper"),
        F.lower("store_name").alias("store_name_lower"),
    )
)

metrics = (
    valid
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w2))
    .withColumn("city_max_sqft", F.max("sqft").over(w3))
    .withColumn("region_max_sqft", F.max("sqft").over(w4))
    .withColumn("first_known_sqft_2", F.first("sqft", ignorenulls=True).over(w5))
)

scored = (
    metrics
    .select(
        "*",
        F.lead("row_seq", 1).over(w6).alias("next_row_seq"),
        F.first("sqft", ignorenulls=True).over(w7).alias("first_known_sqft_3"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupBy("city")
    .pivot("region", ["East", "West"])
    .agg(F.sum("region_max_sqft"))
)
