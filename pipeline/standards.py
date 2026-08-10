"""Official hazard classification standards.

Every threshold here traces to a published government/scientific authority.
Nothing is invented. When a nurse asks "why is this High?", the answer is
"EPA says so" - not "our model said 0.87".

Severity scale is 0-4, aligned with NWS/CDC HeatRisk levels:
    0 Little to None | 1 Minor | 2 Moderate | 3 Major | 4 Extreme

SOURCES
  EPA AQI ............ https://www.airnow.gov/aqi/aqi-basics/
  NWS Heat Index ..... https://www.weather.gov/safety/heat-index
  NWS Wind Chill ..... https://www.weather.gov/safety/cold-wind-chill-chart
  FWI (EFFIS) ........ https://effis.jrc.ec.europa.eu/about-effis/technical-background/fire-danger-forecast
  NWS HeatRisk ....... https://www.wpc.ncep.noaa.gov/heatrisk/  (see NOTE)

NOTE ON HEATRISK
  NWS/CDC HeatRisk (0-4) is the *ideal* standard for this use case: it is
  purpose-built for health, derived from CDC mortality data, and it adjusts
  thresholds per location and date (solving acclimation - a Phoenix patient
  and a Seattle patient have different tolerances).
  We CANNOT compute it: it requires NWS's high-resolution gridded climatology
  plus CDC heat-health data, and is published as a gridded PRODUCT, not a
  formula. Neither Ambee nor Visual Crossing supplies it.
  We therefore use NWS Heat Index, which IS computable from feels-like temp
  and is the basis of NWS heat advisories.
  UPGRADE PATH: ingest HeatRisk directly from NWS.
"""

NONE, MINOR, MODERATE, MAJOR, EXTREME = 0, 1, 2, 3, 4

SEVERITY_NAME = {0: "Little to None", 1: "Minor", 2: "Moderate",
                 3: "Major", 4: "Extreme"}


# --------------------------------------------------------------------------
# EPA Air Quality Index
# --------------------------------------------------------------------------
# Ambee already returns the EPA category string; we verified it matches EPA
# breakpoints exactly (Chicago 335 -> Hazardous, NYC 103 -> USG, etc).
# We map the NUMERIC AQI ourselves so we never depend on vendor strings.
EPA_AQI_BREAKPOINTS = [
    (0, 50, "Good", NONE),
    (51, 100, "Moderate", MINOR),
    (101, 150, "Unhealthy for Sensitive Groups", MODERATE),
    (151, 200, "Unhealthy", MAJOR),
    (201, 300, "Very Unhealthy", EXTREME),
    (301, 10_000, "Hazardous", EXTREME),
]


def classify_aqi(aqi):
    """EPA AQI -> (category, severity). https://www.airnow.gov/aqi/aqi-basics/"""
    if aqi is None:
        return None, None
    for lo, hi, name, sev in EPA_AQI_BREAKPOINTS:
        if lo <= aqi <= hi:
            return name, sev
    return None, None


# --------------------------------------------------------------------------
# NWS Heat Index  (https://www.weather.gov/safety/heat-index)
# --------------------------------------------------------------------------
# Applied to FEELS-LIKE (heat index), never dry-bulb temperature. Heat kills
# through the humidity-adjusted index: Philadelphia at 93F/65%RH feels like
# 110.9F and is more dangerous than Phoenix at 97F/38%RH feeling like 102F.
NWS_HEAT_INDEX = [
    (-999, 79.9, "No Heat Concern", NONE),
    (80, 89.9, "Caution", MINOR),
    (90, 102.9, "Extreme Caution", MODERATE),
    (103, 124.9, "Danger", MAJOR),
    (125, 999, "Extreme Danger", EXTREME),
]


def classify_heat_index(feels_like_f):
    """NWS Heat Index -> (category, severity)."""
    if feels_like_f is None:
        return None, None
    for lo, hi, name, sev in NWS_HEAT_INDEX:
        if lo <= feels_like_f <= hi:
            return name, sev
    return None, None


# --------------------------------------------------------------------------
# NWS Wind Chill  (https://www.weather.gov/safety/cold-wind-chill-chart)
# --------------------------------------------------------------------------
# CAVEAT: NWS Wind Chill Advisory/Warning thresholds are set LOCALLY by each
# forecast office and vary by region (-15F in the upper Midwest vs milder in
# the South). The bands below are a defensible national default keyed to the
# NWS frostbite-time chart. They SHOULD be overridden per tenant.
NWS_WIND_CHILL = [
    (50, 999, "No Cold Concern", NONE),
    (32, 49.9, "Cool", MINOR),
    (16, 31.9, "Cold", MODERATE),
    (-15, 15.9, "Very Cold", MAJOR),
    (-999, -15.1, "Extreme Cold - frostbite risk", EXTREME),
]


def classify_wind_chill(feels_like_f):
    """NWS wind chill -> (category, severity). Applied to FEELS-LIKE."""
    if feels_like_f is None:
        return None, None
    for lo, hi, name, sev in NWS_WIND_CHILL:
        if lo <= feels_like_f <= hi:
            return name, sev
    return None, None


# --------------------------------------------------------------------------
# Fire Weather Index - EFFIS danger classes
# --------------------------------------------------------------------------
# CAVEAT: FWI danger classes are calibrated regionally. EFFIS (European Forest
# Fire Information System) classes are the most widely cited standard and are
# used here as a default. US regional calibrations differ - worth confirming
# with a fire-weather source before production.
EFFIS_FWI = [
    (0, 5.19, "Very Low", NONE),
    (5.2, 11.19, "Low", MINOR),
    (11.2, 21.29, "Moderate", MODERATE),
    (21.3, 37.99, "High", MAJOR),
    (38.0, 999, "Very High to Extreme", EXTREME),
]


def classify_fwi(fwi):
    """FWI -> (category, severity). EFFIS classes."""
    if fwi is None:
        return None, None
    for lo, hi, name, sev in EFFIS_FWI:
        if lo <= fwi <= hi:
            return name, sev
    return None, None


# --------------------------------------------------------------------------
# Pollen / ILI - vendor category strings (Ambee)
# --------------------------------------------------------------------------
POLLEN_SEVERITY = {"Low": NONE, "Moderate": MINOR, "High": MODERATE,
                   "Very High": MAJOR}
ILI_SEVERITY = {"Low": NONE, "Moderate": MODERATE, "High": MAJOR,
                "Very High": EXTREME}


def classify_pollen(risk):
    if risk is None:
        return None, None
    sev = POLLEN_SEVERITY.get(risk)
    return (risk, sev) if sev is not None else (None, None)


def classify_ili(risk):
    if risk is None:
        return None, None
    sev = ILI_SEVERITY.get(risk)
    return (risk, sev) if sev is not None else (None, None)


# --------------------------------------------------------------------------
# Condition -> hazard sensitivity
# --------------------------------------------------------------------------
# Which hazards threaten which patients. Derived from EPA's own definition of
# "Sensitive Groups" (heart or lung disease, older adults, children) plus CDC
# heat-health guidance.
#
# EPA explicitly names AQI 101-150 "Unhealthy for SENSITIVE GROUPS" - the
# standard itself does the condition x hazard match for air quality.
CONDITION_SENSITIVITY = {
    "COPD":         ["air_quality", "wildfire", "pollen", "ili", "heat", "cold"],
    "Asthma":       ["air_quality", "pollen", "wildfire", "cold", "ili"],
    "Hypertension": ["heat", "cold", "air_quality"],   # cardiovascular
    "Diabetes":     ["heat"],                          # thermoregulation, dehydration
    "Arthritis":    ["cold"],
    "Depression":   ["heat", "cold"],                  # isolation; psychotropics
}

# EPA "Sensitive Groups" for air quality: heart or lung disease, older adults
EPA_SENSITIVE_CONDITIONS = {"COPD", "Asthma", "Hypertension"}
EPA_SENSITIVE_AGE = 65

# CDC heat-health: older adults, chronic conditions
CDC_HEAT_SENSITIVE_CONDITIONS = {"COPD", "Hypertension", "Diabetes", "Depression"}
CDC_HEAT_SENSITIVE_AGE = 65

# Comorbidities that amplify specific hazards
COMORBIDITY_AMPLIFIERS = {
    "Smoking": ["air_quality", "wildfire"],   # existing airway damage
    "Obesity": ["heat"],                      # impaired thermoregulation
}
