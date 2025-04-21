import requests
import snowflake.connector

coins_ids, coins_names, coins_symbols = [], [], []
usd_prices, pkr_prices = [], []
market_caps, volumes, descriptions, icons = [], [], [], []

def extract_data(limit=50):
    urls = [
        "https://api.coingecko.com/api/v3/coins/list",
        "https://api.coingecko.com/api/v3/simple/price",
        "https://api.coingecko.com/api/v3/coins/"
    ]

    for url in urls:
        if url == urls[0]:
            response = requests.get(url)
            data = response.json()
            for coin in data[:limit]:
                coins_ids.append(coin['id'])
                coins_names.append(coin['name'])
                coins_symbols.append(coin['symbol'])

        elif url == urls[1]:
            vs_currencies = ["usd", "pkr"]
            ids_str = ",".join(coins_ids)
            params = {"ids": ids_str, "vs_currencies": ",".join(vs_currencies)}
            response = requests.get(url, params=params)
            data = response.json()
            for coin_id in coins_ids:
                coin_data = data.get(coin_id, {})
                usd_prices.append(coin_data.get("usd"))
                pkr_prices.append(coin_data.get("pkr"))

        elif url == urls[2]:
            for coin_id in coins_ids:
                coin_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
                response = requests.get(coin_url)
                if response.status_code == 200:
                    data = response.json()
                    market_data = data.get("market_data", {})
                    market_caps.append(market_data.get("market_cap", {}).get("usd"))
                    volumes.append(market_data.get("total_volume", {}).get("usd"))
                    descriptions.append(data.get("description", {}).get("en", ""))
                    icons.append(data.get("image", {}).get("large", ""))
                else:
                    market_caps.append(None)
                    volumes.append(None)
                    descriptions.append(None)
                    icons.append(None)
    print("extraction done")

        

    # Return all lists as zipped records
    #return list(zip(coins_ids, coins_names, coins_symbols, usd_prices, pkr_prices, market_caps, volumes, descriptions, icons))


def transform_data():
    global usd_prices, pkr_prices, market_caps, volumes, descriptions, icons
    usd_prices = [float(price) if price is not None else 0.0 for price in usd_prices]
    pkr_prices = [float(price) if price is not None else 0.0 for price in pkr_prices]
    market_caps = [float(value) if value is not None else 0.0 for value in market_caps]
    volumes = [float(value) if value is not None else 0.0 for value in volumes]
    descriptions = [desc if desc is not None else "No description" for desc in descriptions]
    icons = [icon if icon is not None else "No icon" for icon in icons]
    print("transformation done")

def load_to_snowflake():
    conn = snowflake.connector.connect(
        user='<username>',
        password='<passward>',
        account=',accountidentifier>',
        warehouse='<warehouse>',
        database='crypto',
        schema='PUBLIC'
    )
    cur = conn.cursor()

    # Create the table (if it doesn't already exist)
    cur.execute("""
        CREATE OR REPLACE TABLE crypto_info (
            id STRING,
            name STRING,
            symbol STRING,
            usd_price FLOAT,
            pkr_price FLOAT,
            market_cap FLOAT,
            volume FLOAT,
            description STRING,
            icon_url STRING
        );
    """)
    print("Table 'crypto_info' created.")

    # Insert the data using parameterized queries
    insert_query = """
        INSERT INTO crypto_info (
            id, name, symbol, usd_price, pkr_price, market_cap, volume, description, icon_url
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """
    
    # Loop through the data to insert rows
    for i in range(0, 50):  # Loop through the first 51 elements (0 to 50)
        # Prepare the values as a tuple for the current coin
        values = (
            coins_ids[i],
            coins_names[i],
            coins_symbols[i],
            usd_prices[i],
            pkr_prices[i],
            market_caps[i],
            volumes[i],
            descriptions[i],
            icons[i]
        )
        
        try:
            # Execute the query with the values passed as parameters
            cur.execute(insert_query, values)
        except Exception as e:
            print(f"Error inserting data for {coins_names[i]}: {e}")

    print("Data inserted into Snowflake successfully.")
    cur.close()
    conn.close()



extract_data(limit=50)
transform_data()
load_to_snowflake()