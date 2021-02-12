# -*- coding: utf-8 -*-
"""
Main class for wrangling data.

One single method for to manage the files.
:copyright: © 2020 Associazione Codice Viola
:license: Apache2, see LICENSE for more details.
"""

import pandas

PREPOSITIONS = [
    "il",
    "lo",
    "la",
    "i",
    "gli",
    "le",
    "di",
    "del",
    "dello",
    "della",
    "dei",
    "degli",
    "delle",
    "a",
    "al",
    "allo",
    "alla",
    "ai",
    "agli",
    "alle",
    "da",
    "dal",
    "dallo",
    "dalla",
    "dai",
    "dagli",
    "dalle",
    "in",
    "nel",
    "nello",
    "nella",
    "nei",
    "negli",
    "nelle",
    "con",
    "col",
    "cóllo",
    "cólla",
    "coi",
    "cogli",
    "cólle",
    "su",
    "sul",
    "sullo",
    "sulla",
    "sui",
    "sugli",
    "sulle",
    "per",
    "pel",
    "pello",
    "pella",
    "pei",
    "pegli",
    "pelle",
]


def italian_title(name):
    """
    Italian titling.

    :param name: The name to be titled
    :return:
    """

    def __prep_filter(e):
        if "'" in e:
            particles = e.casefold().split("'", 1)
            if next((True for _ in PREPOSITIONS if particles[0] in _[:-1]), None):
                return "'".join([particles[0].lower(), particles[1].title()])
            return "'".join([particles[0].title(), particles[1].title()])
        return e.lower() if e.casefold() in PREPOSITIONS else e.capitalize()

    return " ".join(map(__prep_filter, name.split()))


def clean_city_name(entry):
    """
    Clean up the city names.

    @:param entry the city name to clean
    """
    print(entry)
    if type(entry) is float:
        return
    entry = entry[: entry.find(" (")] if " (" in entry else entry
    return italian_title(entry)


def wrangle(src, dest):
    """
    Do all the dirty work.

    @:param src the file path source csv
    @:param dst the file path destination output
    """
    dataframe = pandas.read_csv(src)
    dataframe = dataframe.applymap(clean_city_name)
    dataframe = dataframe.set_index("Città")
    dataframe = dataframe.sort_index()
    dataframe.to_csv(dest, index=True)
    print(f"{src} => {dest}")


def ispra_per_municipality(src, dest):
    """Parse the ISPRA datas from https://annuario.isprambiente.it/sites/default/files/sys_ind_files/indicatori_ada/124/TABELLA%201_PM10_2018_new.xlsx."""
    dataframe = pandas.read_excel(src)
    dataframe = dataframe.drop(
        [
            "Nome della stazione",
            "Unnamed: 0",
            "Unnamed: 1",
            "Tipo di zona",
            "Tipo di stazione",
            "Tecnica di misura",
            "Valore medio1,3 annuo",
            "50° percentile1 ",
            "75° percentile2 ",
            "90,4° percentile2",
            "98°  percentile2 ",
            "99,2° percentile2",
            "Valore massimo2 ",
            "Numero di dati validi",
            "AQD used4",
        ],
        axis=1,
    )
    dataframe = dataframe.drop(dataframe.index[[0]])
    dataframe = dataframe.rename(columns={"Giorni di superamento di 50 µg/m3": "Limit days"})
    dirty_limits = dataframe[dataframe["Limit days"] == "-"]
    dataframe = dataframe.drop(dirty_limits.index)
    dataframe["Limit days"] = dataframe["Limit days"].astype(int)
    dataframe = dataframe[dataframe["Limit days"] >= 35]
    dataframe = dataframe.drop(["Limit days"], axis=1)
    dataframe = dataframe.drop_duplicates(subset=["Comune"])
    dataframe = dataframe.set_index("Comune")
    dataframe = dataframe.sort_index()
    dataframe.to_csv(dest, index=True)
    print(f"{src} => {dest}")


def ispra(src, dest):
    """Parse the ISPRA datas from https://annuario.isprambiente.it/sites/default/files/sys_ind_files/indicatori_ada/124/TABELLA%201_PM10_2018_new.xlsx."""
    dataframe = pandas.read_excel(src)
    dataframe = dataframe.drop(
        [
            "Nome della stazione",
            "Comune",
            "Unnamed: 0",
            "Tipo di zona",
            "Tipo di stazione",
            "Tecnica di misura",
            "Valore medio1,3 annuo",
            "50° percentile1 ",
            "75° percentile2 ",
            "90,4° percentile2",
            "98°  percentile2 ",
            "99,2° percentile2",
            "Valore massimo2 ",
            "Numero di dati validi",
            "AQD used4",
        ],
        axis=1,
    )
    dataframe = dataframe.drop(dataframe.index[[0]])
    dataframe = dataframe.rename(columns={"Unnamed: 1": "Provincia", "Giorni di superamento di 50 µg/m3": "Limit days"})
    dirty_limits = dataframe[dataframe["Limit days"] == "-"]
    dataframe = dataframe.drop(dirty_limits.index)
    dataframe["Limit days"] = dataframe["Limit days"].astype(int)
    dataframe = dataframe[dataframe["Limit days"] >= 35]
    dataframe = dataframe.drop(["Limit days"], axis=1)
    dataframe = dataframe.drop_duplicates(subset=["Provincia"])
    dataframe = dataframe.set_index("Provincia")
    dataframe = dataframe.sort_index()
    dataframe.to_csv(dest, index=True)
    print(f"{src} => {dest}")


wrangle("data/residenza.csv", "output/residenza.csv")
wrangle("data/nascita.csv", "output/nascita.csv")
ispra("data/TABELLA 1_PM10_2018_new.xlsx", "output/ispra.csv")
ispra_per_municipality("data/TABELLA 1_PM10_2018_new.xlsx", "output/ispra_comune.csv")
