from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.year("launch_date") >= 2023))
    .withColumn("product_name_upper", F.upper("product_name"))
    .withColumn("greatest_cost_price", F.greatest("cost", "price"))
)

result = (
    valid
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .rollup(
        F.col("s.country").alias("country"),
        F.col("s.city").alias("city"),
        F.col("a.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("a.cost")), 2).alias("total_cost"),
        F.min(F.col("a.stock_qty")).alias("min_stock_qty"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("brand")
)
