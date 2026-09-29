from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.size("tags") >= 2) & F.array_contains("tags", "bestseller"))
    .select("stock_qty", "supplier_id", "cost")
)
