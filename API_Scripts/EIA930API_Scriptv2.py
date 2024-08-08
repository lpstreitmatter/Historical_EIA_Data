import requests
import pandas as pd
import time

# This script finds the max interface transfer at one time across whole FERC regions
# You will need an EIA API key to download the data (https://www.eia.gov/opendata/register.php). 
# Copy the key into a file named API_Scripts/api_key.txt for use in this script. 

def convert_headers(url, headers):
    '''
        url (str): string with data to pull and api key
        headers (dict): dictionary of headers to provide to api to filter data to pull

        returns url (str) to get filtered data from API
    '''
    for key in headers:
        if type(headers[key]) == str:
            url += "&" + key + "=" + headers[key]
        elif type(headers[key]) == int:
            url += "&" + key + "=" + str(headers[key])
        elif type(headers[key]) == list:
            for head in headers[key]:
                url += "&" + key + "[]=" + head
        elif type(headers[key]) == dict:
            for innerkey in headers[key]:
                for item in headers[key][innerkey]:
                    url += "&" + key + "[" + innerkey + "][]=" + item
        else:
            print("Unrecognized header type", key)
    return url

api_key = open("API_Scripts/api_key.txt", "r").readline()
part1 = "https://api.eia.gov/v2/electricity/rto/interchange-data/data?"
api = "api_key=" + api_key
part1 += api

headers = {
    "frequency": "hourly",
    "data": [
        "value"
    ],
    "facets": {
        "fromba": [
            "ISNE"
        ]
    },
    "start": "2018-08-01T00",
    "end": "2018-08-02T00",
    "sort": [],
    "offset": 0,
    "length": 5000
}
ba_list = pd.read_csv("EIA BA Groupings\\EIA_allBAs.csv")["BAs"]
ferc_mapping = pd.read_csv("EIA-NAERM BA Mapping\\BA-lookup.csv")
ferc_mapping.set_index("EIA BA", inplace=True)
ferc_mapping = ferc_mapping.to_dict()["FERC-Region"]

start = "2021-01-01T00"
end = "2024-01-01T00"
periods = pd.date_range(start = start, end=end, freq="168H")

# Loop through all the API calls
max_dfs = []
for i,period in enumerate(periods):
    print("PERIOD: ", period)
    headers["start"] = str(period).split(" ")[0] + "T00"
    if i == len(periods) - 1:
        headers["end"] = end
    else:
        headers["end"] = str(periods[i+1]).split(" ")[0] + "T00"
    dfs = []
    for ba in ba_list:
        headers["facets"]["fromba"] = [ba]
        url = convert_headers(part1, headers)
        try:
            response = requests.get(url)
            response.raise_for_status()
        except:
            time.sleep(4) # wait a few seconds before retrying API call
            response = requests.get(url)
            response.raise_for_status()
        data = response.json()
        data2 = data['response']['data']
        df = pd.DataFrame(data2, columns = ["period","fromba", "fromba-name", "toba", "toba-name",
                                            "value", "value-units"])
        df = df[["period", "fromba", "toba", "value"]]
        df["fromferc"] = df["fromba"].map(ferc_mapping)
        df["toferc"] = df["toba"].map(ferc_mapping)
        dfs.append(df)
        # Make sure to make fewer than 9,000 calls per hour and under 5 per second
        time.sleep(0.3) #to not over-call the API

    df = pd.concat(dfs, axis=0)
    # to get BA-level interregional transmission data:
    df = df.groupby(["period","fromferc","toferc","fromba","toba"])["value"].sum().to_frame() 
    # to get FERC region-level interregional transmission data: 
    # df = df.groupby(["period","fromferc","toferc"])["value"].sum().to_frame() 
    max_dfs.append(df)
df = pd.concat(max_dfs, axis=0) # this is a df of all timestamps between start and end
# Uncomment to calculate the maximum incident transmission across each FERC interface over the whole period
# df = df.groupby(["fromferc", "toferc"])["value"].apply(lambda x: x.abs().max()).to_frame()

# this produces a very large CSV (300+ GB) at the BA-level
# further analysis done in historical_analysis.ipynb
df.to_csv("EIA_BAlims_2021-2023v2_fullBAs.csv")

