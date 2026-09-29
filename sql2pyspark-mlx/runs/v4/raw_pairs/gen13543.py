from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-07-01", "2024-03-30"))
    .select("status")
    .unionByName(df_shipments.filter(F.col("carrier") == "Flash").select("status"))
    .distinct()
    .orderBy("status")
)
