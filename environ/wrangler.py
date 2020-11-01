# -*- coding: utf-8 -*-
"""
Main class for wrangling data.

One single method for to manage the files.
:copyright: © 2020 Associazione Codice Viola
:license: Apache2, see LICENSE for more details.
"""

import pandas


def clean_city_name(entry):
    """
    Clean up the city names.

    @:param entry the city name to clean
    """
    if " (" in entry:
        return entry[: entry.find(" (")].capitalize()
    else:
        return entry.capitalize()


def wrangle(src, dest):
    """
    Do all the dirty work.

    @:param src the file path source csv
    @:param dst the file path destination output
    """
    dataframe = pandas.read_csv(src)
    dataframe = (
        dataframe.applymap(clean_city_name())
        .drop_duplicates(subset=["Città"])
        .drop("X")
        .set_index("Città")
        .sort_index()
    )
    dataframe.to_csv(dest, index=True)


wrangle("data/residenza.csv", "output/residenza.csv")
wrangle("data/nascita.csv", "output/nascita.csv")
