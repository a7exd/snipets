"""Скрипт для работы с Excel файлами и валидации url адресов в них
 при помощи библиотек pandas и requests."""

import pandas
from urllib.parse import urlsplit
import requests
from pandas import DataFrame
from requests.exceptions import MissingSchema, ConnectionError


def get_dframe(excel_name: str, **kwargs) -> DataFrame:
    return pandas.read_excel(excel_name, **kwargs)


def get_norm_hosts(df: DataFrame, url_column: str) -> set:
    """Функция для получения множества хостов из DataFrame имеющих
    только название домена и доменную зону (example.com)"""

    res = set()
    for site in df[url_column]:
        url = str(site).strip()
        url = urlsplit(url)
        host = url.hostname
        if host is not None:
            host = host.split('.')[-2:]
            norm_host = '.'.join(host)
            res.add(norm_host)
    return res


def clean_dataframe_by_site(df: DataFrame, exclude_sites: set,
                            url_column: str) -> DataFrame:
    """Функция очистки DataFrame от строк, которые имеют в поле url_column
        адреса из множества exclude_sites"""

    res = []
    for index, row in df.iterrows():
        sites = str(row[url_column]).strip().split(' | ')
        hosts = [urlsplit(site).hostname for site in sites
                 if urlsplit(site).hostname is not None]
        norm_hosts = ['.'.join(host.split('.')[-2:]) for host in hosts]
        norm_hosts_set = set(norm_hosts)
        intersec = norm_hosts_set.intersection(exclude_sites)
        if len(intersec) == 0:
            res.append(row)
    return DataFrame(res)


def get_dataframe_with_available_url(df: DataFrame, url_column: str
                                     ) -> DataFrame:
    """Функция отбраковки строк DataFrame
     в зависимости от доступности url адреса в них"""

    res = []
    for index, row in df.iterrows():
        sites = str(row[url_column]).strip().split(' | ')
        for site in sites:
            try:
                response = requests.get(site, timeout=5)
            except (MissingSchema, ConnectionError):
                continue
            else:
                if response.ok:
                    res.append(row)
    return DataFrame(res)
