from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger

STORE_MAIN_URLS = {
    "Almedina": "https://www.almedina.net/catalogsearch/result/?q=",
    "Leya": "https://www.leyaonline.com/pt/pesquisa/pesquisa_ajax.php",
    "Presenca": "https://www.presenca.pt/search?q=",
}


def read_books(file_path: Path) -> pd.DataFrame:
    """Read book database from file into dataframe."""
    df = pd.read_csv(file_path, sep=",")
    df["isbn"] = df["isbn"].astype(str)
    df = df.set_index("isbn", drop=False)

    # initialise values
    df["best_price"] = None
    df["best_store"] = None
    df["discount"] = None
    return df


def read_historical_data(file_path: Path) -> pd.DataFrame:
    """Read historic data from file into a dataframe."""
    df = pd.read_csv(file_path, sep="\t")
    df["isbn"] = df["isbn"].astype(str)
    df = df.set_index("isbn", drop=False)
    return df


def get_html_from_url(url: str, headers: dict = {}) -> BeautifulSoup:
    """Get the HTML from a web page.

    Args:
        url (str): url of the webpage.
        headers (dict, optional): Headers to use in GET request.

    Returns:
        BeautifulSoup: HTML parse tree
    """
    logger.info(f"Get request to URL - {url}")
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    html = BeautifulSoup(response.text, "html.parser")
    return html


def get_price_almedina(store_url: str, book: pd.Series) -> Optional[float]:
    """Get the price of a book for the store Almedina."""
    url = f"{store_url}{book['isbn']}"
    html = get_html_from_url(url)

    message_element = html.find("div", {"class": "message notice"})
    # book not found
    if message_element is not None:
        return None

    price_element = html.find("span", {"data-price-type": "finalPrice"})
    if price_element is not None:
        price = price_element.text.replace(",", ".").replace("€", "").replace(" ", "")
        return float(price)
    return None


def get_price_leya(store_url: str, book: pd.Series) -> Optional[float]:
    """Get the price of a book for the store Leya."""
    form_data = {
        "chave": book["isbn"],
        "pagina": "1",
        "num_prod_pag": "15",
        "ordenar": "0",
        "listagem": "2",
        "categorias": "0",
        "editoras": "0",
    }

    logger.info(f"Making a POST request to {store_url}")
    response = requests.post(store_url, data=form_data)

    html = BeautifulSoup(response.text, "html.parser")
    price_element = html.find("div", {"class": "right"})
    if price_element is not None:
        price = float(
            price_element.text.replace("&#8364;", "")
            .replace(",", ".")
            .replace("€", "")
            .replace(" ", "")
        )
        return price
    return None


def get_price_presenca(store_url: str, book: pd.Series) -> Optional[float]:
    """Get the price of a book for the store Presenca."""
    url = f"{store_url}{book['isbn']}"
    html = get_html_from_url(url)

    price_element = html.find("span", {"style": "font-size:36px;font-weight:600"})
    if price_element is not None:
        price = float(
            price_element.text.replace(",", ".")
            .replace("€", "")
            .replace(" ", "")
            .replace("&euro;", "")
        )
        return price
    return None


def get_price(store: str, store_url: str, book: pd.Series) -> Optional[float]:
    """Get the current price of a book from a given store.

    Args:
        store (str): Name of the store.
        store_url (str): URL of the store.
        book (pd.Series): Series containing information about the book.

    Returns:
        Optional[float]: Price of the book, if found.
    """
    price_functions = {
        "Almedina": get_price_almedina,
        "Leya": get_price_leya,
        "Presenca": get_price_presenca,
    }

    if store not in price_functions.keys():
        raise ValueError(
            f"The store {store} is not found in {set(price_functions.keys())}."
        )

    return price_functions[store](store_url, book)


def update_historical_data(
    df_history: pd.DataFrame, df_books: pd.DataFrame
) -> pd.DataFrame:
    """Update historical dataframe with most recent data.

    Args:
        df_history (pd.DataFrame): Not updated historical dataframe.
        df_books (pd.DataFrame): Dataframe with today's book prices.

    Returns:
        pd.DataFrame: Updated historical dataframe.
    """
    today_date = datetime.today().strftime("%Y-%m-%d")

    for book_series in df_books.itertuples():
        if book_series.Index not in df_history.index:
            df_history.loc[book_series.Index] = book_series

        if book_series.best_price is None:
            continue

        if book_series.best_price <= (
            df_history.loc[book_series.Index, "best_price"] + 0.02
        ):  # round margin
            logger.info(
                f"Best price so far for book  {book_series.title} on store {book_series.best_store}!"
            )

            df_history.loc[book_series.Index, "best_price"] = book_series.best_price
            df_history.loc[book_series.Index, "best_store"] = book_series.best_store
            df_history.loc[book_series.Index, "discount"] = book_series.discount
            df_history.loc[book_series.Index, "best_date"] = today_date

    if today_date not in df_history.columns:
        df_history.insert(loc=9, column=today_date, value=df_books["best_price"])

    return df_history


def scrape_price_from_store(row: pd.Series, store: str, store_url: str) -> pd.Series:
    """Retrieve the book price for a given store and update row with best price/store.

    Args:
        row (pd.Series): Data of the book to scrape
        store (str): Name of the store
        store_url (str): URL of the store

    Returns:
        pd.Series: Updated book data
    """
    logger.info(f"Scraping book {row['title']}.")
    scraped_price = get_price(store, store_url, row)

    if scraped_price is None:
        logger.warning(
            f"Could not retrieve price for store {store} and book {row['title']}."
        )
        return row

    if (row["best_price"] is None) or (
        scraped_price <= (row["best_price"] + 0.02)
    ):  # round margin
        row["best_price"] = scraped_price
        row["discount"] = round((1 - scraped_price / row["default_price"]) * 100)

        # if there are several stores with same price
        if (row["best_store"] is not None) and (scraped_price == row["best_price"]):
            row["best_store"] = row["best_store"] + "," + store
        else:
            row["best_store"] = store
    return row


def scrape_books(
    path_books_to_scrape: Path,
    path_historic_data: Path,
) -> None:
    """Scrape current prices of books in several stores and update
    historical dataset.

    Args:
        path_books_to_scrape (Path): File path to books to scrape file.
        path_historic_data (Path): File path to historical data file.
    """
    df_books = read_books(path_books_to_scrape)
    df_history = read_historical_data(path_historic_data)

    for store, store_url in STORE_MAIN_URLS.items():
        logger.info(f"Scraping store {store}.")
        df_books = df_books.apply(
            scrape_price_from_store, args=(store, store_url), axis=1
        )

    df_history_updated = update_historical_data(df_history, df_books)
    df_history_updated.to_csv(path_historic_data, sep="\t", index=False)


if __name__ == "__main__":
    path_books_to_scrape = Path("data/book_collection.csv")
    path_historical_data = Path("data/historic_data.csv")
    
    scrape_books(path_books_to_scrape, path_historical_data)
