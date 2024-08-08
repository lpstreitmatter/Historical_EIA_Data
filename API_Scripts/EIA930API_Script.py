import requests
import pandas as pd
import time

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

api_key = open("api_key.txt", "r").readline()
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
start = "2020-01-01T00"
end = "2021-01-01T00"
periods = pd.date_range(start = start, end=end, freq="168H")

# Loop through all the API calls I want
max_dfs = []
for ba in ba_list:
    print("BALANCING AUTHORITY: ", ba)
    headers["facets"]["fromba"] = [ba]

    dfs = []
    for i,period in enumerate(periods):
        headers["start"] = str(period).split(" ")[0] + "T00"
        if i == len(periods) - 1:
            headers["end"] = end
        else:
            headers["end"] = str(periods[i+1]).split(" ")[0] + "T00"

        url = convert_headers(part1, headers)
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data2 = data['response']['data']
        df = pd.DataFrame(data2, columns = ["period","fromba", "fromba-name", "toba", "toba-name",
                                            "value", "value-units"])
        df = df[["period", "fromba", "toba", "value"]]
        dfs.append(df)
        # Make sure to make fewer than 9,000 calls per hour and under 5 per second
        time.sleep(0.25) #to not over-call the API

    df = pd.concat(dfs, axis=0)
    max_df = df.groupby(["fromba", "toba"])["value"].apply(lambda x: x.abs().max()).to_frame()
    max_dfs.append(max_df)
df = pd.concat(max_dfs, axis=0)

df.to_csv("EIA_BAlims_2020.csv")
