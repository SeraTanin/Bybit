import csv
import requests


def check_to_repeat():
    lst = []
    with open('Data.csv', encoding='utf-8', newline="") as file:
        reader = csv.reader(file, delimiter=";")
        try:
            for r in reader:
                if r[1] == 'Open':
                    pass
                else:
                    lst.append(r[1])
        except IndexError:
            pass
    return lst


def get_pair_name():
    url = "https://api-testnet.bybit.com/v5/market/mark-price-kline?category=linear&symbol=BTCUSDT&interval=D"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload).json()

    pair = response['result']['symbol']
    return pair


for i in range(10):
    if i == 4:
        continue
    else:
        print(i)


if __name__ == '__main__':
    check_to_repeat()
    # print(get_pair_name())

