from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Grocery")
    .select("launch_date")
    .intersect(
        df_stores
        .filter(~F.col("region").isin(["Central", "North"]))
        .select("opened_date")
    )
)
