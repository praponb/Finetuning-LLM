from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy(F.col("least_revenue_units").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("units", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("units").desc())
w5 = Window.orderBy("units_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-14").cast("date"))
    .select("*", F.least("revenue", "units").alias("least_revenue_units"))
)

latest = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

latest_store_sales = (
    latest
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select("id", "product_id", "units", "rn")
)

scored = (
    latest_store_sales
    .filter(F.col("rn") != 1)
    .select(
        "id",
        "rn",
        F.lag("units").over(w3).alias("prev_units"),
        F.lead("units").over(w3).alias("next_units"),
        F.row_number().over(w3).alias("event_seq"),
        F.dense_rank().over(w4).alias("units_dense_rank"),
    )
)

enriched = (
    scored
    .withColumn("running_max_next_units", F.max("next_units").over(w5))
    .withColumn("running_count_next_units", F.count("next_units").over(w5))
    .withColumn("running_sum_next_units", F.sum("next_units").over(w5))
)

result = (
    enriched
    .select(
        "id",
        "next_units",
        "event_seq",
        "units_dense_rank",
        "running_max_next_units",
        "running_sum_next_units",
    )
    .orderBy(F.col("running_max_next_units").desc_nulls_last())
)
