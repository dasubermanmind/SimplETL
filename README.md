 ____  _  _      ____  _     _____ _____  _    
/ ___\/ \/ \__/|/  __\/ \   /  __//__ __\/ \   
|    \| || |\/|||  \/|| |   |  \    / \  | |   
\___ || || |  |||  __/| |_/\|  /_   | |  | |_/\
\____/\_/\_/  \|\_/   \____/\____\  \_/  \____/

By dasubermanmind13
Y does this look like crapp....?


## Get your data loaded
This library allows you to spin up an ETL process without you having to think too much on it so you can get to 
more important things like analyzing your data or fortnite 


## Where we load things?
By default we setup a Postgres docker container and transform targetting this database


## Installation
COMING SOON!

But will look something like this

```bash
pip install simpletl
```

## Formats that work ROADMAP
CSV - [X] 

[] - JSON -- In Progress Now. 
[] - XML (eww)
[] - Excel -- This will be interesting


Load layer - [] Default is going to be Postgres Connect to DB[X] 

Transform Layer - [] WIP. Need to figure out what transform I will always perform

Abstract Class - [] This one and only!

## Integrations with SIMPETL

1. Jupyter Notebook Integration [] What does this mean? My idea is simple. After we run an ingest we then spawn a Jupyter Notebook with the dataframe already loaded and maybe a couple operations loaded into the notebook. 

1. Scheduler. Use case so lets say you want to run a specific ingest at 1523 every second monday of the month. Well you could set up a cron job to get this done. But this feels like it should be native to SimplETL.

1. UI. One day Ill get to a React Frontend Dashboard. Just not today...

1. Using Multiple databases (endpoints)

## Contributing
Disclaimer: I am new to OSS so if you see something that I can improve please don't hesitate to reach out! Also, dont be an ass there's a difference between being helpful and giving good critical advice vs being a rude jackass. Shouldn't be hard to be nice. 

```
eliot.ost@protonmail.com
```

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
