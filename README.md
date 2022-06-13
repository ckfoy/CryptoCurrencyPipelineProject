# CryptoCurrencyPipelineProject

The purpose of this project is to build a data pipeline for cryptocurrency data using Apache Spark. The currency pricing data was collected
from the CryptoCompare website and loaded into Apache Spark using Python. The data was then inserted into a MongoDB local standalone cluster.

![image](https://user-images.githubusercontent.com/80182167/173341098-5298056d-3aeb-4029-bf84-280a03166a1b.png)

Hardware and Software:

All programs were run on a Windows 10 64-bit operating system. PySpark version 3.2.1 and MongoDB version 3.11.4 were used for this 
project. The MongoDB Compass GUI version 1.30.1 was used to view the final results with its local host, standalone cluster.

How the Code Works:

First, the API key from Crypto Compare is input to pull the hourly historical data from the website. The limit is set to 2,000 which spans back about 3 months,
and more data can be collected by repeating this call. I collected data from 8 different crypto currencies: Bitcoin, Ethereum, Terra, Solano, XRP, Avalanche,
Binance Coin, Cosmos. 
The datetime format from the API needs to be cleaned up so the data is input into a pandas dataframe with the index set as a datetime type with each hour labeled.

![image](https://user-images.githubusercontent.com/80182167/173342135-5f933d62-183e-42b4-80c0-735a5b855a23.png)

That data is then converted into a PySpark dataframe where the schema is defined. The datetime type is now a TimestampType and the rest of the columns are FloatTypes.

![image](https://user-images.githubusercontent.com/80182167/173342262-f797b37e-0068-4997-80e8-0c0373172c6e.png)

That data is then saved and appended to a MongoDB on a standalone, local cluster called “Crypto".

![image](https://user-images.githubusercontent.com/80182167/173342396-a7bf4dfa-110a-4904-bde3-3893ff04e24a.png)



This line plot created in Python shows the historical hourly data for Bitcoin over the last 3 months (December 2021 – March 2022).

![image](https://user-images.githubusercontent.com/80182167/173342594-e6bebeb6-c370-4170-bef6-dc9143c36b3c.png)
