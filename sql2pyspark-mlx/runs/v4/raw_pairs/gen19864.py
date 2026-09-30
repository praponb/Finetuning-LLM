from pyspark.sql import functions as F

result = df_accounts.filter((F.col("balance") <= 68000) & F.col("customer_id").isNotNull())
