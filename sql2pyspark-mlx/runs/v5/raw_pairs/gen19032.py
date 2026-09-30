from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id").orderBy("revenue", "id")
w3 = Window.orderBy("revenue", "id")
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.max("units").alias("max_store_sale_units"))

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-06-14").cast("date")) & F.col("product_id").isin([1, 5, 8, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.when(F.col("revenue") >= 3300, "High").when(F.col("revenue") >= 1100, "Medium").otherwise("Low").alias("revenue_band"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "store_id",
        "revenue",
        "revenue_band",
        F.lag("units").over(w).alias("prev_units"),
        F.lead("units").over(w).alias("next_units"),
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.row_number().over(w).alias("row_seq"),
    )
)

metrics = (
    enriched
    .withColumn("prev_next_units", F.lag("next_units", 1, 0).over(w2))
    .withColumn("revenue_bucket", F.ntile(4).over(w3))
)

valid_store_sales = (
    metrics
    .filter(F.col("revenue_bucket") < 7)
    .select(
        "id",
        "store_id",
        "revenue",
        "next_units",
        "row_seq",
        "prev_next_units",
        "revenue_bucket",
        F.round((F.col("next_units") / F.col("prev_next_units")), 2).alias("next_units_per_prev_next_units"),
    )
)

result = (
    valid_store_sales
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "next_units",
        "revenue_bucket",
        "next_units_per_prev_next_units",
        F.col("max_store_sale_units"),
    )
    .orderBy(F.col("store_id").desc_nulls_first())
)
