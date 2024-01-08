import yaml


def read_yaml(file_path):
    '''
        Reads data from config yaml file

        Parameters : 
            file_path : File path to yaml config file

        Returns : 
            Dict with config data 

    '''
    try : 
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)

    except Exception as e : 
        raise Exception(f'Error occurred while loading data from yaml file with following exception {e}')


def read_csv_data(spark, file_path):
    '''
        Reads data in csv format

        Parameters : 
            spark : Spark instance 
            file_path : File path containing csv data
        
        Returns : 
            Dataframe : Spark Dataframe containing input csv file data
    '''
    try : 
        return spark.read.option('header', 'True').option('inferSchema', 'true').csv(file_path)

    except Exception as e : 
        raise Exception(f'Error occurred while reading data from file with following exception {e}')



def write_csv_data(df, file_path):
    '''
        Write output dataframe to csv file

        Parameters : 
            df : Dataframe containing the results
            file_path : Path to file in which data needs to be stored

        Returns : 
            None
    '''
    try : 
        df = df.repartition(1)
        df.write.format('csv').mode('overwrite').option('header', 'true').save(file_path)
    
    except Exception as e : 
        raise Exception(f'Error occurred while writing df data to csv file with following exception {e}')
        


