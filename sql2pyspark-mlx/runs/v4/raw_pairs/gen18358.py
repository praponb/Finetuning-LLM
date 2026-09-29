from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") != "Globex")
    .select("supplier_id")
    .unionByName(
        df_products
        .filter(F.col("brand").isin(["Globex", "Initech", "Acme"]))
        .select("supplier_id")
    )
)
