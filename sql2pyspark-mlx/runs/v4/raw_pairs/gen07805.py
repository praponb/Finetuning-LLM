from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("is_south", "id").rowsBetween(-6, -1)
w_ordered = Window.partitionBy("region").orderBy("sqft_filled", "id")
w3 = Window.partitionBy("region").orderBy("greatest_is_south_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("region").orderBy(F.col("is_south").desc(), "id")

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-03-31"))) & F.col("region").isNotNull() & F.col("region").isNotNull())
    .withColumn("is_south", F.when(F.col("region") == "South", 1).otherwise(0))
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "left_anti")
)

base = (
    eligible
    .filter(F.col("sqft_filled") >= 70)
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "is_south",
        "sqft_filled",
        F.greatest("is_south", "sqft").alias("greatest_is_south_sqft"),
    )
)

base_stores = (
    base
    .filter(F.col("opened_date") >= F.lit("2023-06-25").cast("date"))
    .select(
        "id",
        "region",
        "sqft",
        "is_south",
        "sqft_filled",
        "greatest_is_south_sqft",
        F.coalesce("greatest_is_south_sqft", F.lit(0)).alias("greatest_is_south_sqft_filled"),
    )
)

result = (
    base_stores
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_6_prior"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.sum("greatest_is_south_sqft_filled").over(w3).alias("running_greatest_is_south_sqft_filled"),
        F.first("is_south", ignorenulls=True).over(w4).alias("first_known_is_south"),
        F.ntile(3).over(w_running).alias("is_south_bucket"),
    )
)
