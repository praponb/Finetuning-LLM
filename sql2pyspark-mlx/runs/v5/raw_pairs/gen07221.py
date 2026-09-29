from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("category") == "Sports"))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.initcap(F.trim("brand")).alias("brand_title"),
    )
)

result = (
    filtered
    .groupBy("supplier_id")
    .pivot("brand", ["Globex", "Umbrella", "Initech"])
    .agg(F.max("cost").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "supplier_id",
        F.col("Globex_value_sum").alias("globex_value_sum"),
        F.col("Globex_cnt").alias("globex_cnt"),
        F.col("Umbrella_value_sum").alias("umbrella_value_sum"),
        F.col("Umbrella_cnt").alias("umbrella_cnt"),
        F.col("Initech_value_sum").alias("initech_value_sum"),
        F.col("Initech_cnt").alias("initech_cnt"),
    )
)
