#Libraries import
import requests
import pandas as pd
import os


#Function to get token from txt file
def get_token():
    path_ = os.path.join(os.getcwd(), "token_api.txt")
    if os.path.exists(path_):
        with open(f"{path_}",encoding="utf-8",mode="r") as txt_file:
            token = txt_file.read().strip()
            return token
    else:
        print("Token file not found.")
        return None

#Function to get data from API
def get_data(url, params, token):
    response = requests.get(url,params=params,headers={"token":token})
    if response.status_code == 200:
        return response.status_code, response.json()
    else:
        print(f"Error: {response.status_code} : {response.text}")
        return None

#Function to save data to csv
def save_data_to_csv(df):
    if df is not None:
        df.to_csv(path_or_buf=os.path.join(os.getcwd(), "noaa_data.csv"), index=False)
        print("Operation was successful.")
    else:
        print("No data to save.")

#Function to convert json into a dataframe
def to_pandas(response_data):
    #Transform data into a pandas dataframe
    df = pd.DataFrame(response_data["results"])
    #Drop the column attributes (values from this column are not important)
    df.drop("attributes",inplace=True,axis=1)
    return df

#Function to map the city to the referring station
def map_city(x):
    station = x["station"].split(":")[1]
    if "BR0015" in station:
        return "Cuiaba, BR"
    elif "BR0025" in station:
        return "Curitiba, BR"
    else:
        return "Florianopolis, BR"

#Function to transform the dataframe
def df_transformation(df):
    df["date"] = pd.to_datetime(df.apply(lambda x: x["date"].split("T")[0].strip(),axis=1))
    df["value"] = df["value"].astype(float)
    df["city"] = df.apply(map_city,axis=1)
    return df


#URL base
URL_BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"


#API parameters
params_get = {"datasetid":"GSOM",
        "startdate":"1990-10-01",
        "enddate":"2000-01-22",
        "limit":500,
        "datatypeid":"PRCP",
        "includemetadata":False,
        "locationid":["CITY:BR000007","CITY:BR000008","CITY:BR000009"]
        }

#Get token from txt file
token = get_token()


if token is not None:
    #Store data and status code from api two variables
    response_status_code, response_data = get_data(url=URL_BASE, params=params_get,token=token)
    if "results" in response_data:
        df = to_pandas(response_data=response_data)


#Transforming dataframe
df_final = df_transformation(df=df)


#Call function to save data to csv file
save_data_to_csv(df=df_final)