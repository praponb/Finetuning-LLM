from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("quantity").between(91, 172))
        .select("id")
    )
)
