# -*- coding: utf-8 -*-
import asyncio

from prefect import task, flow
from prefect.artifacts import create_table_artifact

import yfinance as yf

DICT_OF_TICKER = {
    "Hang-Send-Index": "HIS",
    "Crude-Oil": "CL=F",
    "Brent-Oil": "BZ=F",
    "Natural-Gas": "NG=F",
    "US-Dollar-Index": "DX=F",
    "EUR-USD": "EURUSD=X",
    "GPB-USD": "GBPUSD=X",
    "GPB-EUR": "GBPEUR=X",
    "EUR-JPY": "EURJPY=X",
    "DAX-Performance-Index": "GDAXI",
    "CAC-40-Index": "FCHI",
    "MOEX-Russia-Index": "IMOEX.ME",
    "Nasdaq-Futures": "NQ=F",
    "Dow-Futures": "YM=F",
    "Bitcoin-USD": "BTC-USD",
    "Etherum-USD": "ETH-USD",
    "Apple": "AAPL",
    "Microsoft": "MSFT",
    "Meta": "META",
}


@task
def pull_data_sync(ticker: str):
    # Pull data from Yahoo Finance
    return yf.Ticker(ticker).history(period="max").reset_index()


@task
async def pull_data_async(list_of_ticker: list[str]):
    tasks = [asyncio.create_task(pull_data_sync(ticker)) for ticker in list_of_ticker]
    return await asyncio.gather(*tasks)


@task
def clean_data(data):
    data = data.sort_values(by="Date", ascending=False)
    data["Date"] = data["Date"].astype(str)
    return data.to_dict(orient="records")


@task
def upload_to_artifact(key, description, data):
    return create_table_artifact(key=key, description=description, table=data)


@flow
async def pipeline():
    for ticker_name, ticker_icon in DICT_OF_TICKER.items():
        data = pull_data_sync(ticker_icon)
        data_cleaned = clean_data(data)
        upload_to_artifact(f"{ticker_name.lower()}-data", f"{ticker_name.lower()}({ticker_icon.lower()}) historical data", data_cleaned)
