# Processing Data with Spark

The goal of this project is to infer qualitative data regarding the car accidents in New York City. The given dataset includes information 
about date, time, location, borough, number of people injured and killed based (pedestrians, cyclist, motorist...), contributing factors 
and vehicle types.

- Q1. Number of lethal accidents per week throughout the entire dataset.

- Q2. Number of accidents and percentage of number of deaths per contributing factor in the dataset.

  *I.e., for each contributing factor, we want to know how many accidents were due to that contributing factor and what percentage of these accidents were also lethal.*

- Q3. Number of accidents and average number of lethal accidents per week per borough.

  *I.e., for each borough, we want to know how many accidents there were in that borough each week, as well as the average number of lethal accidents that the borough had per week.*
  
The version of the deployments are 2.12 for Scala and 2.4.4 for Spark. All of them
can be obtained in the [official webpage](https://spark.apache.org/downloads.html).
The dataset used for this project can be downloaded from [Kaggle](https://www.kaggle.com/new-york-city/nypd-motor-vehicle-collisions).

## Usage

```shell
usage: CarAccidents [-f <FILE>] [-m <ADDRESS>] [-q <QUESTION>] [-s <X>]
 -f,--file <FILE>           CSV file
 -m,--master <ADDRESS>      spark master address
 -q,--question <QUESTION>   data to access
 -s,--show <X>              show X results
```
