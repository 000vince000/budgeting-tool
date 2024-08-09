import os
import pandas as pd
from pandasai import Agent

# Sample DataFrame
sales_by_country = pd.DataFrame({
    "country": ["United States", "United Kingdom", "France", "Germany", "Italy", "Spain", "Canada", "Australia", "Japan", "China"],
    "revenue": [5000, 3200, 2900, 4100, 2300, 2100, 2500, 2600, 4500, 7000]
})

# By default, unless you choose a different LLM, it will use BambooLLM.
# You can get your free API key signing up at https://pandabi.ai (you can also configure it in your .env file)
os.environ["PANDASAI_API_KEY"] = "$2a$10$wqRNlp9vws.J2xj7Ecb/eObCJlH3W.KhNi4qbZH7wdM.txkhTgjNm"

# agent = Agent(sales_by_country)
# agent.chat('Which are the top 5 countries by sales?')

df = pd.read_csv("finance-2024.csv")
agent=Agent(df)

agent.chat("plot sorted absolute value of amount total in log scale by categories with different colors for July, with total shown in each category")

agent.chat("take the top category in terms of absolute sum and plot the absolute value by month")

agent.chat("take the second highest category in terms of absolute sum and plot the absolute value by month")
