from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("region", "city").orderBy(F.col("is_south").desc(), "id")
w5 = Window.partitionBy("region")
w6 = Window.partitionBy("region").orderBy(F.col("sqft_share").desc(), "id")

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-03-14").cast("date")) & F.col("region").isNotNull())
    .withColumn("is_south", F.when(F.col("region") == "South", 1).otherwise(0))
    .withColumn("region_upper", F.upper("region"))
)

scored = (
    base
    .withColumn("sqft_bucket", F.ntile(3).over(w))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w2))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4))
    .withColumn("is_south_bucket", F.ntile(3).over(w4))
)

scored_stores = (
    scored
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w5)), 4).alias("sqft_share_2"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

result = (
    scored_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "is_south",
        "region_upper",
        "sqft_bucket",
        "first_known_region",
        F.when(F.col("region").rlike("^(West|South)$"), "GROUP_A").when(F.col("region").rlike("^(North|East)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("region_zone"),
    )
    .orderBy(F.col("city").desc_nulls_last(), "sqft")
)
