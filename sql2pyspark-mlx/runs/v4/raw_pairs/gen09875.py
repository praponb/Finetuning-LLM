from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sqft").desc_nulls_last(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")

filtered = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-12-26").cast("date")) & (F.col("units") == 27))
    .select("*", F.date_add("sale_date", 19).alias("sale_date_plus_19d"))
)

enriched = (
    filtered
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.sqft"))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_store_sales = (
    latest
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

result = (
    latest_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "sale_date_plus_19d",
        "sqft",
        "rank_in_group",
    )
    .orderBy("id", F.col("sale_date_plus_19d").desc())
    .limit(20)
)
