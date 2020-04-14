## process JHU COVID-19 daily by-state data for ICL model v2
## Data Source: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series
## Also uses US Census data for population age distribution
## Python 3.7

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from state_abbrev import us_state_abbrev, abbrev_us_state
from tqdm import tqdm
from os import system

## WARNING
## WARNING
## WARNING
## removes existing file backups, backs up current files, fetches new files

system("remove-destination time_series_covid19_confirmed_US.csv time_series_covid19_confirmed_US.csv.bak")
system("remove-destination time_series_covid19_deaths_US.csv time_series_covid19_deaths_US.csv.bak")
system("rm time_series_covid19_confirmed_US.csv")
system("rm time_series_covid19_deaths_US.csv")
system("wget https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv")
system("wget https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv")

# Census data is static and need only be processed once
# system("wget http://www2.census.gov/programs-surveys/popest/datasets/2010-2018/national/totals/nst-est2018-alldata.csv")

us_deaths = pd.read_csv("time_series_covid19_deaths_US.csv")
us_cases = pd.read_csv("time_series_covid19_confirmed_US.csv")
us_pop = pd.read_csv("nst-est2018-alldata.csv")

FIXED_IFR = 0.0109655 ## uses average of EU country IFRs due to lack of US testing

ignore = ["Grand Princess", "Diamond Princess", "American Samoa", 
          "Virgin Islands", "Guam", "Northern Mariana Islands"]

lines = []
for state in tqdm(set(us_deaths['Province_State'])):
    line = []
    if state not in ignore:
        line.append(state)
        line.append(int(us_pop[us_pop.NAME == state]["POPESTIMATE2018"]))
        line.append(FIXED_IFR)
        lines.append(line)

colnames = ["country","popt","ifr"]
ifrs = pd.DataFrame(lines, columns=colnames)
ifrs.to_csv("popt_ifr_us.csv")

dates = list(us_deaths)[12:]
first = datetime.strptime(dates[0],"%m/%d/%y")
delt = timedelta(days=1)

lines = []
for state in tqdm(set(us_deaths['Province_State'])):
    if state not in ignore:
        #print(state)
        case_prev = 0
        death_prev = 0

        for d in dates: #reformat existing dates
            dtg = datetime.strptime(d,'%m/%d/%y')
            line = [dtg.strftime("%d/%m/%Y"), dtg.month, dtg.day, dtg.year]

            case_cur = np.sum(us_cases[us_cases['Province_State'] == state][d])
            line.append(case_cur-case_prev)
            case_prev = case_cur

            death_cur = np.sum(us_deaths[us_deaths['Province_State'] == state][d])
            line.append(death_cur - death_prev)
            death_prev = death_cur

            line.append(state)
            line.append(us_state_abbrev[state])
            line.append(us_state_abbrev[state])
            line.append(int(us_pop[us_pop.NAME == state]["POPESTIMATE2018"]))
            line.append(str(dtg.year)+ "." + str(int(dtg.strftime('%j'))/365)[2:5])
            lines.append(line)

        for i in range(1,31): # add backdated "no cases, no deaths" days (30 of them)
            dtg = first - i*delt
            line = [dtg.strftime("%d/%m/%y"), dtg.month, dtg.day, dtg.year, 0, 0, state, us_state_abbrev[state],
                   us_state_abbrev[state]]
            line.append(int(us_pop[us_pop.NAME == state]["POPESTIMATE2018"]))
            line.append(str(dtg.year)+ "." + str(int(dtg.strftime('%j'))/365)[2:5])
            lines.append(line)

            colnames=(["DateRep", "day", "month", "year", "Cases",
          "Deaths", "Countries.and.territories", "geoId", "countryterritoryCode", "popData2018","t"])

data_out = pd.DataFrame(lines, columns=colnames)
data_out.to_csv("COVID-19-up-to-date_us.csv", index=False)
set(data_out['Countries.and.territories'])
