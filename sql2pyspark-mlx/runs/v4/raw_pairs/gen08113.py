from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

filtered = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2024-01-09").cast("date"))
    .select(
        "*",
        F.month("last_restock").alias("last_restock_month"),
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    latest
    .filter(F.col("product_id").isin([2, 5, 9]))
    .select("*", F.last_day("last_restock").alias("last_restock_month_end"))
)

joined = (
    cleaned
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.sqft"), F.col("s.store_name"))
)

result = (
    joined
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "last_restock_month",
        "last_restock_plus_months",
        "sqft",
        "store_name",
        F.when(F.col("store_name").rlike("^(Riverside|Old Town)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("store_name_zone"),
        F.col("sum_store_sale_revenue"),
    )
    .orderBy(F.col("last_restock").desc())
)
