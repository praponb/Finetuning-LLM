from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy("sqft", "id")
w3 = Window.partitionBy("product_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-08-14").cast("date")) & F.col("store_id").isin([1, 3]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.date_trunc("month", "sale_date").alias("sale_date_period"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.city").isin(["Bangkok", "Berlin", "New York"]))
    .select("src.*", F.col("b.sqft"), F.col("b.opened_date"))
)

latest = (
    with_dims
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "sale_date",
        "sale_date_period",
        "sqft",
        F.ntile(5).over(w2).alias("sqft_bucket"),
        F.last("sqft", ignorenulls=True).over(w3).alias("last_known_sqft"),
    )
)

stats = (
    enriched
    .groupBy(F.when(F.col("sqft") >= 14766, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.min("sale_date_period").alias("first_sale_date_period"),
        F.min("sale_date_period").alias("first_sale_date_period_2"),
    )
    .filter(F.col("distinct_ids_2") > 1)
)

result = (
    stats
    .select(
        "sqft_tier",
        "distinct_ids",
        "distinct_ids_2",
        "first_sale_date_period",
        "first_sale_date_period_2",
    )
    .orderBy(F.col("distinct_ids_2").desc())
)
