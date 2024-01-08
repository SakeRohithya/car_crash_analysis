# CASE_STUDY : Car Crash Analysis

[Data folder](https://github.com/SakeRohithya/car_crash_analysis/tree/ebf1da19c3cc6f91b49867242217c74d8accecc5/Data) has 6 csv files.Please use the [data dictionary](https://github.com/SakeRohithya/car_crash_analysis/blob/ebf1da19c3cc6f91b49867242217c74d8accecc5/Data%20Dictionary.xlsx) to understand the dataset and then develop your approach to perform below analytics.

Analytics:

a. Application should perform below analysis and store the results for each analysis.

  1. Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
  
  2. Analysis 2: How many two wheelers are booked for crashes?
  
  3. Analysis 3: Which state has highest number of accidents in which females are involved?
  
  4. Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
  
  5. Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
  
  6. Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
  
  7. Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
  
  8. Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

## Procedure to run the project

### Setup
Download and install pySpark 
```
pip install pyspark 
```
Clone the github repo 
```
git clone https://github.com/SakeRohithya/car_crash_analysis
```
### Modifications
Edit [config.yaml](https://github.com/SakeRohithya/car_crash_analysis/blob/258369467d8cdcadf8219af0270f18981d3a8bdb/config.yaml) according to your folder paths.
- input_resource : Input folder location to read data into dfs
- output_destination : Output folder location to store results in csv format
```
input_source :
  Charges : ./Data/Charges_use.csv
  Damages : ./Data/Damages_use.csv
  Endorse : ./Data/Endorse_use.csv
  Primary_Person : ./Data/Primary_Person_use.csv
  Restrict : ./Data/Restrict_use.csv
  Unit : ./Data/Units_use.csv

output_destination :
  analysis1 : ./Output/analysis1_male_killed_count
  analysis2 : ./Output/analysis2_two_wheeler_booked
  analysis3 : ./Output/analysis3_state_with_highest_female_accidents
  analysis4 : ./Output/analysis4_top_5to15_vehicle_ids
  analysis5 : ./Output/analysis5_top_ethinc_vehicle_body_wise
  analysis6 : ./Output/analysis6_top_5_drivercode_alcohol_factor
  analysis7 : ./Output/analysis7_crash_ids_no_damage_car_insurance
  analysis8 : ./Output/analysis8_top5_make_ids_speed_charge_top10_color_top25_state

```
### python files
1. [utilities.py](https://github.com/SakeRohithya/car_crash_analysis/blob/258369467d8cdcadf8219af0270f18981d3a8bdb/Code/utilities.py) : Handles loading yaml file given config file path , reading csv data into df , writes df into csv file
2. [queries.py](https://github.com/SakeRohithya/car_crash_analysis/blob/258369467d8cdcadf8219af0270f18981d3a8bdb/Code/queries.py) : Invokes utilities file , Loads the data into dfs , Perform query analyis logic  and stores the result in output folder
3. [main.py](https://github.com/SakeRohithya/car_crash_analysis/blob/258369467d8cdcadf8219af0270f18981d3a8bdb/main.py) : Main file invokes queries file and execute the analysis queries

Run the application by using the following command
```
spark-submit --master local main.py
```







