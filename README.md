# COVID-19 Visualizer

This app fills a gap in what I've seen of publicly available COVID-19 data. From many public authorities they only report the current day's data. This is useful, but without historical data lacks context. As well, when the media creates charts, they are many times abstract (i.e., the valuable flatten the curve charts), or only taking a snapshot. This seeks to make this data both publicly available and easily accessible. It would not exist if not for the fine folks at [Johns Hopkins](https://github.com/CSSEGISandData/COVID-19) collating this data: they are the real superheroes. The app is built with [Dash](https://plot.ly/dash/), which is an excellent library for building interactive visualizations like R's Shiny package. There's an ETL here that puts the Johns Hopkins data into an SQL database (it's built with PostgreSQL, because that's what I know); steal and use it.

## Caveats

The charts are only as good as their sources. I rely entirely on the Johns Hopkins dataset, but there are snags in that dataset. To wit,

1. Sometimes, columns are empty. That's ok. If the Confirmed dataset has an empty column I skip that day. Otherwise, return a 0 for the Deaths and Recovered values. If the chart looks weird, let me know, that's likely the reason.
2. National values are an aggregate of state or province level values, where applicable. There are dataset caveats: Washington  once reported by county, as well as the state total. I now add that to the ETL for the state level where applicable.
3. Data updates at 2AM EDT.
4. The infrastructure is shoddy as hell. If it falls over, it falls over. If it gets traffic I'll invest time and money there.

## Future Goals

1. Better infrastructure. You really don't want to know what it looks like right now.
2. Allow comparison of country and state values.
