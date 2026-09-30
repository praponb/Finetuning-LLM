from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "shipped_date", F.initcap(F.trim("carrier")).alias("carrier_title"))
)
