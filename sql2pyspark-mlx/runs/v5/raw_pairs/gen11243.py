from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("category") == "Electronics"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.substring("product_name", 1, 4).alias("product_name_prefix"),
    )
)

result = (
    valid
    .groupBy("brand")
    .pivot("category", ["Grocery", "Electronics", "Home"])
    .agg(F.sum("price").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "brand",
        F.col("Grocery_value_sum").alias("grocery_value_sum"),
        F.col("Grocery_item_count").alias("grocery_item_count"),
        F.col("Electronics_value_sum").alias("electronics_value_sum"),
        F.col("Electronics_item_count").alias("electronics_item_count"),
        F.col("Home_value_sum").alias("home_value_sum"),
        F.col("Home_item_count").alias("home_item_count"),
    )
)
