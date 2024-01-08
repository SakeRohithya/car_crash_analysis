from pyspark.sql import SparkSession
from Code.queries import CarCrashAnalysis



def queries_execution():
    '''
    Execute Car Crash Analysis queries
    '''
    # Create spark session
    spark = SparkSession.builder.appName('CarCrashAnalysis').getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Path to the configuration file
    config_file_path = 'config.yaml'

    # Create an instance of the CarCrashAnalysis class
    car_analysis_class = CarCrashAnalysis(spark , config_file_path)

    # 1. Find the number of crashes (accidents) in which number of persons killed are male?
    print('1. crash_male_killed_count:', car_analysis_class.crash_male_killed_count() , end='\n\n')

    # 2. How many two-wheelers are booked for crashes?
    print('2. two_wheelers_booked:', car_analysis_class.two_wheelers_booked(), end='\n\n')

    # 3. Which state has the highest number of accidents in which females are involved?
    print('3. state_highest_females_involved:', car_analysis_class.state_highest_females_involved(), end='\n\n')

    # 4. Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print('4. top_5to15_vehicle_ids:', car_analysis_class.top_5to15_vehicle_ids(), end='\n\n')

    # 5. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print('5. top_ethinc_vehicle_body_wise:' , car_analysis_class.top_ethinc_vehicle_body_wise(), end='\n\n')

    # 6. Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the
    # contributing factor to a crash (Use Driver Zip Code)
    print('6. top_5_drivercode_alcohol_factor:', car_analysis_class.top_5_drivercode_alcohol_factor(), end='\n\n')

    # 7. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
    # and car avails Insurance
    print('7. crash_ids_no_damage_car_insurance:', car_analysis_class.crash_ids_no_damage_car_insurance(), end='\n\n')

    # 8. Determine the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
    # Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
    # offences (to be deduced from the data)
    print('8. top5_make_ids_speed_charge_top10_color_top25_state:', car_analysis_class.top5_make_ids_speed_charge_top10_color_top25_state(), end='\n\n')


if __name__ == '__main__':

    queries_execution() 