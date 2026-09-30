from pyspark.sql import functions as F

max_cost = df_products.filter(F.col("category").isin(["Home", "Books"])).agg(F.max("cost")).first()[0]

result = (
    df_products
    .filter(F.col("cost") >= max_cost)
    .select("id", "cost", "supplier_id")
)
