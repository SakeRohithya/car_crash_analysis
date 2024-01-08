from pyspark.sql import  Window
from pyspark.sql.functions import col, dense_rank , sum , count
from Code.utilities import read_yaml , read_csv_data  , write_csv_data 


class CarCrashAnalysis:


    def __init__(self, spark , config_file_path):
        '''
        Initialize the class and load data into dfs

        Parameters : 
            spark : SparkSession object
            config_file_path : Path to YAML configuartion file

        '''  
        
        self.spark = spark
        self.config_file_path = config_file_path
        self._dfs_loading()


    def _dfs_loading(self):
        '''
        Load Dataframes from csv files based on input paths in configuration
        '''

        try : 
            config = read_yaml(self.config_file_path)

            input_source_paths = config['input_source']
            self.outputs_source_path = config['output_destination']

            self.charges_df = read_csv_data(self.spark, input_source_paths['Charges'])
            self.damages_df = read_csv_data(self.spark, input_source_paths['Damages'])
            self.endorse_df = read_csv_data(self.spark, input_source_paths['Endorse'])
            self.primary_person_df = read_csv_data(self.spark, input_source_paths['Primary_Person'])
            self.units_df = read_csv_data(self.spark, input_source_paths['Unit'])
            self.restrict_df = read_csv_data(self.spark, input_source_paths['Restrict'])

        except Exception as e:
            raise Exception(f'Error occurred while loading data into dataframes with following exception : {e}')


    def crash_male_killed_count(self):
        '''
        Analysis 1 : Get Count of Crashes in which male are killed 

        Returns : 
            Number of crashes in which male are killed
        '''

        try : 
            # Filter primary_person_df to include only males who are killed
            crash_male_killed_count = self.primary_person_df \
                .filter((col('PRSN_GNDR_ID')=='MALE') & (col('DEATH_CNT')== 1)).select('CRASH_ID').distinct().count()

            # Create dataframe for male_killed_count to write into csv file
            crash_male_killed_count = self.spark.createDataFrame([(crash_male_killed_count,)], [ 'MALE_KILLED_COUNT' ])

            # Write the result Dataframe to csv file 
            write_csv_data(crash_male_killed_count, self.outputs_source_path['analysis1'])

            # Returns count of Crashes in which male are killed
            return [i[0] for i in crash_male_killed_count.collect()]

        except Exception as e:
            raise Exception(f'Error getting count of Crashes in which male are killed with following exception : {e}')


    def two_wheelers_booked(self):
        '''
        Analysis 2 : Get count of two wheeler booked for crashes

        Returns : 
            Count of two wheeler booked for crashes
        '''

        try : 
            # Filter units_df to include only Motorcycle values in VEH_BODY_STYL_ID
            two_wheelers_booked_df = self.units_df \
                .filter(col('VEH_BODY_STYL_ID').like('%MOTORCYCLE%')).count()

            # Create dataframe for two wheeler booked count to write into csv file
            two_wheelers_booked_df = self.spark.createDataFrame([(two_wheelers_booked_df,)], [ 'TWO_WHEELERS_BOOKED_COUNT' ])

            # Write the result Dataframe to csv file 
            write_csv_data(two_wheelers_booked_df, self.outputs_source_path['analysis2'])

            # Returns count of two wheeler booked for crashes
            return [i[0] for i in two_wheelers_booked_df.collect()]
        
        except Exception as e:
            raise Exception(f'Error getting count of two wheeler booked for crashes with following exception : {e}')

    def state_highest_females_involved(self):
        '''
        Analysis 3 : Get the State with highest number of females involved in accident

        Returns :
            List containing the state with the highest number of females involved in accidents.
        '''

        try : 
            # Filter primary_person_df to include only female with valid state ids
            state_wise_females_involved_df = self.primary_person_df \
                .filter((col('PRSN_GNDR_ID') == 'FEMALE') & ~(col('DRVR_LIC_STATE_ID').isin(['NA', 'Unknown', 'Other']))) \
                .groupBy('DRVR_LIC_STATE_ID').agg(count('*').alias('FEMALES_ACCIDENT_COUNT'))

            # Window Specification to rank states by female involved count and filter rank to 1
            spec = Window().orderBy(col('FEMALES_ACCIDENT_COUNT').desc())
            state_highest_females_involved_df = state_wise_females_involved_df \
                .withColumn('FEMALES_INVOLVED_HIGHEST_STATE', dense_rank().over(spec)) \
                .filter(col('FEMALES_INVOLVED_HIGHEST_STATE')==1)

            # Write the result Dataframe to csv file 
            write_csv_data(state_highest_females_involved_df , self.outputs_source_path['analysis3'])

            # Return the state with highest number of females involved in accident
            return [i[0] for i in state_highest_females_involved_df.collect()]

        except Exception as e:
            raise Exception(f'Error getting the State with highest number of accidents involving females with following exception : {e}')

    def top_5to15_vehicle_ids(self):
        '''
        Analysis 4 : Get Top 5 to 15 Vehicle make ids contributing to largest number of injuries including death

        Returns : 
            List containing top 5 to 15 Vehicle make ids contributing to largest number of injuries including death
        '''

        try : 
            # Filter units_df to include veh_make_id with not null values and group by VEH_MAKE_ID and aggregate on sum of injury and death count
            top_5to15_vehicle_ids_df  = self.units_df \
                .filter(~(col('VEH_MAKE_ID').isin(['NA', 'UNKNOWN', 'OTHER (EXPLAIN IN NARRATIVE)']))) \
                .groupBy('VEH_MAKE_ID').agg(sum(col('TOT_INJRY_CNT') + col('DEATH_CNT')).alias('TOTAL_INJRY_DEATH_CNT'))

            # Window Specification to rank vehicle_ids by total injury+death count and filter rank from 5 to 15
            spec = Window().orderBy(col('TOTAL_INJRY_DEATH_CNT').desc())
            top_5to15_vehicle_ids_df = top_5to15_vehicle_ids_df \
                .withColumn('TOTAL_INJRY_DEATH_CNT_RANK', dense_rank().over(spec)) \
                .filter((col('TOTAL_INJRY_DEATH_CNT_RANK') >= 5) & (col('TOTAL_INJRY_DEATH_CNT_RANK') <= 15))

            # Write the result Dataframe to csv file 
            write_csv_data(top_5to15_vehicle_ids_df , self.outputs_source_path['analysis4'])

            # Return top 5 to 15 Vehicle make ids contributing to largest number of injuries including death
            return [i[0] for i in top_5to15_vehicle_ids_df.collect()]

        except Exception as e:
            raise Exception(f'Error getting the State with highest number of accidents involving females with following exception : {e}')

        

    def top_ethinc_vehicle_body_wise(self):
        '''
        Analysis 5 : Get Top ethinc user group for each unique body style 
        
        Returns : 
            List containing top ethinc user group for each unique body style
        '''

        try :
            # Filter primary_person_df to include PRSN_ETHNICITY_ID with not null values and select certain columns on units_df , primary_person_df
            units_filtered_df = self.units_df \
                .select('CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID')
            primary_person_filtered_df = self.primary_person_df.filter(~(col('PRSN_ETHNICITY_ID').isin(['NA', 'UNKNOWN' ,'OTHER']))) \
                .select('CRASH_ID','UNIT_NBR','PRSN_ETHNICITY_ID')

            # Inner Join above filtered units_filtered_df , primary_person_filtered_df on CRASH_ID and UNIT_NBR
            units_primary_person_df = units_filtered_df \
                .join(primary_person_filtered_df, on=['CRASH_ID', 'UNIT_NBR'], how='inner')

            # Group by units_primary_person_df['VEH_BODY_STYL_ID' , 'PRSN_ETHNICITY_ID] and aggregate on total count
            top_ethinc_vehicle_body_wise = units_primary_person_df \
                .groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').agg(count('*').alias('TOTAL_CNT'))

            # Window Specification to get top ethnic user for each veh_body_style_id 
            spec = Window().partitionBy('VEH_BODY_STYL_ID').orderBy(col('TOTAL_CNT').desc())
            top_ethinc_vehicle_body_wise = top_ethinc_vehicle_body_wise \
                .withColumn('RANK',dense_rank().over(spec))\
                .filter(col('RANK')==1).select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID','TOTAL_CNT')

            # Write the result Dataframe to csv file 
            write_csv_data(top_ethinc_vehicle_body_wise , self.outputs_source_path['analysis5'])

            #Returns top ethinc user group for each unique body style
            return [(i[0] , i[1]) for i in top_ethinc_vehicle_body_wise.collect()]

        except Exception as e:
            raise Exception(f'Error getting top ethinc user group for each unique body style with following exception : {e}')



    
    def top_5_drivercode_alcohol_factor(self):
        '''
        Analysis 6 : Get Top 5 driver zip codes with highest number of crashes first contributing factor as alcohol

        Returns : 
            List containing top 5 driver zip codes with highest number of crashes first contributing factor as alcohol
        '''

        try : 
            # Filter primary_person_df to include DRVR_ZIP not null values and PRSN_ALC_RSLT_ID positive value
            primary_person_filtered_df = self.primary_person_df \
                .filter((~col('DRVR_ZIP').isNull()) & (col('PRSN_ALC_RSLT_ID')=='Positive'))
            
            # Group by 'DRVR_ZIP' and aggregate the total count 
            top_5_drivercode_alcohol_factor = primary_person_filtered_df \
                .groupBy('DRVR_ZIP').agg(count('*').alias('TOTAL_CRASH_COUNT'))
            
            # Window Specification to get top 5 driver zip codes with alcohol positive values and highest number of crashes
            spec = Window().orderBy(col('TOTAL_CRASH_COUNT').desc())
            top_5_drivercode_alcohol_factor = top_5_drivercode_alcohol_factor \
                .withColumn('RANK', dense_rank().over(spec)) \
                .filter(col('RANK')<=5)

            # Write the result Dataframe to csv file 
            write_csv_data(top_5_drivercode_alcohol_factor , self.outputs_source_path['analysis6'])

            # Returns top 5 driver zip codes with highest number of crashes first contributing factor as alcohol
            return [i[0] for i in top_5_drivercode_alcohol_factor.collect()]

        except Exception as e:
            raise Exception(f'Error getting top 5 driver zip codes with highest number of crashes first contributing factor as alcohol with following exception : {e}')
        


    def crash_ids_no_damage_car_insurance(self):
        '''
        Analysis 7 : Get Count of unique crash_ids with No damage property , Damage level >4 and car avails insurance

        Returns : 
            (Int) count of unique crash_ids with No damage property , Damage level >4 and car avails insurance
        '''

        try : 
            # Filter units_df and damages_df to include values like No damage property , Damage level >4 , Car avails Insurance
            units_filtered_df = self.units_df \
                .filter((col('FIN_RESP_TYPE_ID').like('%INSURANCE%')) & ( (col('VEH_DMAG_SCL_1_ID').like('%4%')) | (col('VEH_DMAG_SCL_2_ID').like('%4%')) )) \
                .select('CRASH_ID')
            damages_filtered_df = self.damages_df \
                .filter((col('DAMAGED_PROPERTY').like('%NONE%')) | (col('DAMAGED_PROPERTY').like('%NO DAMAGE%'))) \
                .select('CRASH_ID')

            # Join units_filtered_df , damages_filtered_df on crash_id and get unique crash_id count
            damages_units_df = units_filtered_df \
                .join(damages_filtered_df , on = 'CRASH_ID' , how = 'inner').distinct().count()

            # Create dataframe for DISTINCT_CRASH_IDS_COUNT to write into csv file
            damages_units_crash_count_df = self.spark.createDataFrame([(damages_units_df,)], [ 'DISTINCT_CRASH_IDS_COUNT' ])

            # Write the result Dataframe to csv file 
            write_csv_data(damages_units_crash_count_df , self.outputs_source_path['analysis7'])

            # Returns count of unique crash_ids with No damage property , Damage level >4 and car avails insurance
            return [i[0] for i in damages_units_crash_count_df.collect()]
        
        except Exception as e:
            raise Exception(f'Error getting count of unique crash_ids with No damage property , Damage level >4 and car has insurance with following exception : {e}')
        
        

    def top5_make_ids_speed_charge_top10_color_top25_state(self):
        '''
        Analysis 8 : Get Top 5 vehicle_make_ids drivers charged with speeding offences , licensed used top 10 vehicle colours 
        where car licensed with top 25 states with highest number of offences

        Returns :
            List of top 5 vehicle_make_ids satisfying given conditions
        '''
        try : 
            # Filter charges_df to inculde SPEED related offences , primary_person_df to exclude unlicensed drivers
            charges_speed_filtered_df = self.charges_df \
                .filter(col('CHARGE').like('%SPEED%')).select('CRASH_ID','UNIT_NBR','CHARGE')
            primary_person_licensed_df = self \
                .primary_person_df.filter(~col('DRVR_LIC_CLS_ID').like('UNLICENSED')).select('CRASH_ID','UNIT_NBR', 'DRVR_LIC_CLS_ID')

            # Top 10 Vehicle_color : 1.Filter units_df to include not null VEH_COLOR_ID values , Group by VEH_COLOR_ID and aggregate on count
            units_top10_color_df = self.units_df \
                .filter(col('VEH_COLOR_ID')!='NA') \
                .groupBy('VEH_COLOR_ID').agg(count('*').alias('VEH_COLOR_COUNT'))
            # Top 10 Vehicle_color : 2.Window specification to get top 10 vehicle color used
            spec_color = Window().orderBy(col('VEH_COLOR_COUNT').desc())
            units_top10_color_df = units_top10_color_df \
                .withColumn('TOP10_VEH_COLOR', dense_rank().over(spec_color)).filter(col('TOP10_VEH_COLOR')<=10)

            # Top 25 states : 1.Filter VEH_LIC_STATE_ID to exclude not null values , Groupby VEH_LIC_STATE_ID and aggegate on count
            units_top25_state_df = self.units_df \
                .filter(col('VEH_LIC_STATE_ID')!='NA') \
                .groupBy('VEH_LIC_STATE_ID').agg(count('*').alias('VEH_STATE_COUNT'))
            # Top 25 states : 2.Window specification to get top 25 states involved in highest number of crashes
            spec_state = Window().orderBy(col('VEH_STATE_COUNT').desc())
            units_top25_state_df = units_top25_state_df \
                .withColumn('TOP25_VEH_STATE', dense_rank().over(spec_state)).filter(col('TOP25_VEH_STATE')<=25)

            # Filter units_df to include only top 10 vehicle color and top 25 states and select 'CRASH_ID' , 'UNIT_NBR', 'VEH_MAKE_ID' columns
            units_filtered_color_state_df = self.units_df \
                .filter(col('VEH_COLOR_ID').isin([i[0] for i in units_top10_color_df.collect()])) \
                .filter(col('VEH_LIC_STATE_ID').isin([i[0] for i in units_top25_state_df.collect()])) \
                .select('CRASH_ID' , 'UNIT_NBR', 'VEH_MAKE_ID')

            # Inner Join units_filtered_color_state_df , charges_speed_filtered_df , primary_person_licensed_df on CRASH_ID , UNIT_NBR
            final_units_charges_licensed_drivers_df = units_filtered_color_state_df \
                .join(charges_speed_filtered_df , on=['CRASH_ID' , 'UNIT_NBR'], how='inner') \
                .join(primary_person_licensed_df , on=['CRASH_ID' , 'UNIT_NBR'], how='inner')

            # Group final_units_charges_licensed_drivers_df VEH_MAKE_ID and aggeagte on count
            final_units_charges_licensed_drivers_df = final_units_charges_licensed_drivers_df \
                .groupBy('VEH_MAKE_ID').agg(count('*').alias('VEH_MAKE_ID_COUNT'))
            
            # Window Specification to get top 5 vehicle_make_ids 
            spec_make_id = Window().orderBy(col('VEH_MAKE_ID_COUNT').desc())
            final_units_charges_licensed_drivers_df = final_units_charges_licensed_drivers_df \
                .withColumn('RANK', dense_rank().over(spec_make_id)).filter(col('RANK')<=5)

            # Write the result Dataframe to csv file 
            write_csv_data(final_units_charges_licensed_drivers_df, self.outputs_source_path['analysis8'])

            # Returns top 5 vehicle_make_ids satisfying given conditions
            return [i[0] for i in final_units_charges_licensed_drivers_df.collect()]

        except Exception as e:
            raise Exception(f'Error List of top 5 vehicle_make_ids satisfying given conditions with following exception : {e}')



   
