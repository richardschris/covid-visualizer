# COVID-19 Visualizer

This app fills a gap in what I've seen of publicly available COVID-19 data. From many public authorities they only report the current day's data. This is useful, but without historical data lacks context. As well, when the media creates charts, they are many times abstract (i.e., the valuable flatten the curve charts), or only taking a snapshot. This seeks to make this data both publicly available and easily accessible. It would not exist if not for the fine folks at [Johns Hopkins](https://github.com/CSSEGISandData/COVID-19) collating this data: they are the real superheroes. The app is built with [Dash](https://plot.ly/dash/), which is an excellent library for building interactive visualizations like R's Shiny package. There's an ETL here that puts the Johns Hopkins data into an SQL database (it's built with PostgreSQL, because that's what I know); steal and use it.

## Caveats

The charts are only as good as their sources. I rely entirely on the Johns Hopkins dataset, but there are snags in that dataset. To wit,

1. There currently (as of 4/12) are some issues with (at least) New York and Serbia data. So while the noble folks at Johns Hopkins are doing their best, errors exist. I'll update the data as that gets fixed.
2. The derivative view (day-to-day difference) should be taken as a trend over time. The day to day variations don't matter too much, what matters is that those day to day variations add up to a downward slope.

## Future Goals

1. Better infrastructure. You really don't want to know what it looks like right now.
2. Allow comparison of country and state values.
3. Allow more options for visualization (combine moving average and derivative? log derivative?)
4. Make the ETL process more repeatable and easy to understand. As JHU has changed their data, so has the ETL changed in response, but it's complicated and has unnecessary features.
