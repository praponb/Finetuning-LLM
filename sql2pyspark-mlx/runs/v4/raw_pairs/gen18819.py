from pyspark.sql import functions as F

min_quantity = df_inventory.agg(F.min("quantity")).first()[0]

result = (
    df_inventory
    .filter(F.col("quantity") < min_quantity)
    .select("id", "quantity", "product_id")
)
