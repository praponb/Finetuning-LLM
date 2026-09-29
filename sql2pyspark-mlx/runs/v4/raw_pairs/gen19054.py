from pyspark.sql import functions as F

max_revenue = df_store_sales.filter(F.col("revenue") < 2200).agg(F.max("revenue")).first()[0]

result = (
    df_store_sales
    .filter(F.col("revenue") >= max_revenue)
    .select("id", "revenue")
)
