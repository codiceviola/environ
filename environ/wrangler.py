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
    dataframe = dataframe.drop_duplicates(subset=["Città"])
    dataframe = dataframe.set_index("Città")
    dataframe = dataframe.sort_index()
    dataframe.to_csv(dest, index=True)
    print(f"{src} => {dest}")


wrangle("data/residenza.csv", "output/residenza.csv")
wrangle("data/nascita.csv", "output/nascita.csv")
