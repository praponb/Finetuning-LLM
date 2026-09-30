from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "UPS"]))
    .select("delivered_date")
    .intersect(df_store_sales.select("sale_date"))
    .orderBy(F.col("delivered_date").desc())
)
