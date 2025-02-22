import enum

states = [
    "bw",
    "by",
    "be",
    "br",
    "hb",
    "hh",
    "he",
    "mv",
    "ni",
    "nw",
    "rp",
    "sl",
    "sn",
    "st",
    "sh",
    "th"
]

class State(str, enum.Enum):
    BADEN_WUETTEMBERG = "bw"
    BAYERN = "by"
    BERLIN = "be"
    BRANDENBURG = "br"
    BREMEN = "hb"
    HAMBURG = "hh"
    HESSEN = "he"
    MECKLENBURG_VORPOMMERN = "mv"
    NIEDERSACHSEN = "ni"
    NORDRHEIN_WESTFALEN = "nw"
    RHEILAND_PFALZ = "rp"
    SAARLAND = "sl"
    SACHSEN = "sn"
    SACHSEN_ANHALT = "st"
    SCHLESWIG_HOLSTEIN = "sh"
    THUERINGEN = "th"
