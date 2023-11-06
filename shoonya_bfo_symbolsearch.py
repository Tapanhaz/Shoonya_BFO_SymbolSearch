# -*- coding: utf-8 -*-
"""
    :: Description :: Search scrips in shoonya bfo symbolmaster.
    :: License :: MIT
    :: Author :: Tapan Hazarika
    :: Created :: On Monday Novenber 06, 2023
"""
__author__ = "Tapan Hazarika"
__license__ = "MIT"


import os
import logging
import requests
import polars as pl
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
from functools import lru_cache
from typing import Literal, Union


class ShoonyaBFOMaster:
    bfo_url = "https://api.shoonya.com/BFO_symbols.txt.zip"

    def __init__(self, hard_refresh=False) -> None:
        self.hard_refresh = hard_refresh
        self.filepath = os.path.join(os.path.dirname(__file__), "Shoonya_BFO_Modified_Master.csv")
        self.current_date = datetime.now().date()

        if self.hard_refresh:
            self.load_master.cache_clear()
        self.load_master()
    
    def is_latest(self) -> bool:
        file_time = datetime.fromtimestamp(os.path.getmtime(self.filepath)).date()
        logging.info("File modify date :: {}".format(file_time))
        return file_time == self.current_date

    def download_master(self):
        try:
            res = requests.get(self.bfo_url)
            res.raise_for_status()
            if res.status_code == 200:
                with ZipFile(BytesIO(res.content), "r") as zip_file:
                    file_1 = zip_file.namelist()[0]
                    extension = file_1.split('.')[-1]
                    with zip_file.open(file_1) as file:
                        if extension == "txt" or extension == "csv":
                            df = pl.read_csv(
                                        source= file.read(), 
                                        infer_schema_length=10000,
                                        truncate_ragged_lines= True, 
                                        has_header= True
                                        ).with_columns(
                                            pl.col(
                                                ["Token", "LotSize", "StrikePrice", "TickSize"]
                                                ).cast(pl.Utf8)
                                        )
                            return df
                        else:
                            print(f"Unsupported File Type :: {extension}")
        except Exception as e:
            print(f"Error Downloading :: {e}")
    
    def load_master_from_file(self)-> pl.DataFrame:
        df = pl.read_csv(source= self.filepath,
                         dtypes= {
                                "Exchange" : pl.Utf8,
                                "Token" : pl.Utf8,
                                "LotSize" : pl.Utf8,
                                "Symbol" : pl.Utf8,
                                "Symbol_1" : pl.Utf8,
                                "TradingSymbol" : pl.Utf8,
                                "Expiry" : pl.Utf8,
                                "Instrument" : pl.Utf8,
                                "OptionType" : pl.Utf8,
                                "StrikePrice" : pl.Utf8,
                                "TickSize" : pl.Utf8
                            }
                        )
        return df
    
    @lru_cache(maxsize=None)
    def load_master(self)-> pl.DataFrame:
        if os.path.exists(self.filepath):
            if not self.is_latest():
                df = self.download_master()
                if df.is_empty():
                    logging.info("Download failed. Using Old SymbolMaster.")
                    df = self.load_master_from_file()
                else:
                    df = self.prepare_data(df)
                    if not df.is_empty():
                        df.write_csv(self.filepath)
            else:
                if self.hard_refresh:
                    df = self.download_master()
                    df = self.prepare_data(df)
                    if not df.is_empty():
                        df.write_csv(self.filepath)
                else:
                    df = self.load_master_from_file()
        else:
            df = self.download_master()
            df = self.prepare_data(df)
            if not df.is_empty():
                df.write_csv(self.filepath)
        return df

    @staticmethod
    def prepare_data(dataframe: pl.DataFrame)-> pl.DataFrame: 

        df = dataframe.lazy().with_columns(
                                    pl.when(
                                        pl.col("TradingSymbol").str.contains(
                                            "SENSEX50"
                                            )
                                        ).then(
                                            pl.lit("SENSEX50")
                                            ).otherwise(
                                                pl.col(
                                                    "TradingSymbol"
                                                       ).str.extract(
                                                                pattern= r'^(.*?)(\d)'
                                                                )
                                                            ).alias(
                                                            "Symbol_1"
                                                        )
                                                            ).select(
                                                                [
                                                                    "Exchange",
                                                                    "Token",
                                                                    "LotSize", 
                                                                    "Symbol",
                                                                    "Symbol_1",
                                                                    "TradingSymbol",
                                                                    "Expiry",
                                                                    "Instrument",
                                                                    "OptionType",
                                                                    "StrikePrice",
                                                                    "TickSize"
                                                                    ]
                                                                ).collect()
        
        return df
    
    def get_expiry(
            self,
            symbol: str=None,
            instrument: Literal["FUTIDX", "FUTSTK", "OPTIDX", "OPTSTK"]= "OPTIDX",
            expirytype: Literal["near", "next", "far", "all"]= "near"
            )-> Union[str, list]:
        df = self.load_master()
        try:
            if symbol:
                all_expiry= df.lazy().select(
                                            ["Symbol_1", "Expiry","Instrument"]
                                        ).filter(
                                            (pl.col("Symbol_1") == symbol.upper())
                                            &
                                            (pl.col("Instrument") == instrument)

                                        ).with_columns(
                                            pl.col("Expiry").str.to_date(format= "%d-%b-%Y")
                                        ).select(
                                            "Expiry"
                                        ).filter(
                                            pl.col("Expiry") >= self.current_date
                                        ).unique().sort(
                                            by="Expiry"
                                            ).with_columns(
                                                pl.col("Expiry").dt.strftime(format= "%d-%b-%Y").str.to_uppercase()
                                            ).collect()
            
            if expirytype == "near":
                return all_expiry.item(0,0)
            elif expirytype == "next":
                return all_expiry.item(1,0)
            elif expirytype == "far":
                return all_expiry.item(2,0)
            elif expirytype == "all":
                return all_expiry.to_series().to_list()
        except (IndexError, Exception) as e:
            logging.debug("Error Fetching Expiry :: {}".format(e)) 
    
    def get_tradingsymbol(
                    self,
                    symbol: str,
                    instrument: Literal["FUTIDX", "FUTSTK", "OPTIDX", "OPTSTK"],
                    expiry: str,
                    optiontype: str= "XX",        
                    strikeprice: Union[int, str]= 0.0                       
                    )-> str:
        df = self.load_master()
        try:
            tsym = df.lazy().select(
                                  ["Symbol_1", "TradingSymbol", "Expiry","Instrument", "OptionType", "StrikePrice"]  
                                ).filter(
                                    (pl.col("Symbol_1") == symbol.upper())
                                    &
                                    (pl.col("Instrument") == instrument)
                                    &
                                    (pl.col("Expiry") == expiry)
                                    &
                                    (pl.col("OptionType") == optiontype)
                                    &
                                    (pl.col("StrikePrice") == str(float(strikeprice)))
                                ).select(
                                    "TradingSymbol"
                                ).collect().item(0, 0)
            return tsym
        except (IndexError, Exception) as e:
            logging.debug("Error Fetching TradingSymbol :: {}".format(e))
    
    def get_token(
                self,
                symbol: str= None,
                tradingsymbol: str=None,
                instrument: Literal["FUTIDX", "FUTSTK", "OPTIDX", "OPTSTK"]= "OPTIDX",
                expiry: str= None,
                optiontype: str= "XX",        
                strikeprice: Union[int, str]= 0.0 
                )-> str:
        df = self.load_master()
        try:
            if symbol and not tradingsymbol:
                tkn = df.lazy().select(
                                      ["Token", "Symbol_1",  "Expiry", "Instrument", "OptionType", "StrikePrice"]  
                                    ).filter(
                                        (pl.col("Symbol_1") == symbol.upper())
                                        &
                                        (pl.col("Instrument") == instrument)
                                        &
                                        (pl.col("Expiry") == expiry)
                                        &
                                        (pl.col("OptionType") == optiontype)
                                        &
                                        (pl.col("StrikePrice") == str(float(strikeprice)))
                                    ).select(
                                        "Token"
                                    ).collect().item(0, 0)
                return tkn
            elif tradingsymbol:
                tkn= df.lazy().select(
                                            ["Token", "TradingSymbol"]
                                        ).filter(
                                            (pl.col("TradingSymbol") == tradingsymbol.upper())
                                        ).select(
                                            "Token"
                                        ).collect().item(0, 0)
                
                return tkn            
        except (IndexError, Exception) as e:
            logging.debug("Error Fetching Token :: {}".format(e))
    
    def get_strikediff(
                    self,
                    symbol: str= None,
                    )-> float:
        df = self.load_master()

        try:
            strikediff= df.lazy().select(
                                        ["Symbol_1", "StrikePrice"]
                                    ).with_columns(
                                        pl.col("StrikePrice").cast(dtype= pl.Float64)
                                    ).filter(
                                        (pl.col("Symbol_1") == symbol.upper())
                                        &
                                        (pl.col("StrikePrice") > 0)
                                    ).select(
                                        "StrikePrice"
                                    ).unique().sort(by="StrikePrice").collect().to_series().diff(null_behavior="drop").abs().min()

            return strikediff            
        except (IndexError, Exception) as e:
            logging.debug("Error Fetching Strikediff :: {}".format(e))
    
    def get_lotsize(
                    self,
                    symbol: str= None,
                    )-> float:
        try:
            df = self.load_master()

            lotsize = df.lazy().select(
                                        ["LotSize", "Symbol_1"]    
                                    ).filter(
                                        pl.col("Symbol_1") == symbol
                                    ).select(
                                        "LotSize"
                                    ).collect().item(0, 0)
            
            return lotsize
        except (IndexError, Exception) as e:
            logging.debug("Error Fetching Lotsize :: {}".format(e))



