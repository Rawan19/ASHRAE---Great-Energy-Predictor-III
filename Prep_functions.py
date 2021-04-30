def imports():
    import pandas 
    import numpy as np
def fill_na_num_weather(weather_f):
    import pandas
    
    #Forward filling missing data in the weather dataset +-24 hours
   
    weather_f["timestamp"] = pandas.to_datetime(weather_f["timestamp"])
    weather_f["day"] = weather_f["timestamp"].dt.day
    weather_f = weather_f.sort_values(by = ["site_id", "timestamp"])

    weather_f.fillna(method= "ffill", inplace=True, limit=12)
    weather_f.fillna(method = "bfill", inplace=True, limit=12)
    # mean per site id, per day
    # for cloud coverage, values must be discrete
    fill_lib = round(weather_f.groupby(["site_id","day"])["cloud_coverage"].transform("mean") )
    weather_f["cloud_coverage"].fillna(fill_lib, inplace=True)
    
    #for other columns
    missing_cols = [col for col in weather_f.columns if weather_f[col].isna().any()  and col!=   "cloud_coverage"]
    fill_lib = weather_f.groupby(["site_id","day"])[missing_cols].transform("mean") 
    weather_f.fillna(fill_lib, inplace=True)

    ##for sites with missing data during day, fill with mean of site
    missing_cols = [col for col in weather_f.columns if weather_f[col].isna().any() ]
    fill_lib = weather_f.groupby(["site_id"])[missing_cols].transform("mean") 
    weather_f.fillna(fill_lib, inplace=True)
    return weather_f

def merge(metre, building, weather_f):
  import pandas as pandas
  import numpy as np
  metre["timestamp"] = pandas.to_datetime(metre["timestamp"])
  building.drop(["floor_count", "year_built"], axis=1, inplace=True)
  building_metres = pandas.merge(metre, building, how = "left", on="building_id")
  train = pandas.merge(building_metres, weather_f, how= "left", on=["site_id", "timestamp"], validate="many_to_one")
  del building
  del metre
  del weather_f
  return train


def reduce_mem_usage(df, verbose=True):
    import pandas as pandas
    import numpy as np
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    start_mem = df.memory_usage().sum() / 1024**2    
    for col in df.columns:
        col_type = df[col].dtypes
        if col_type in numerics:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)    
    end_mem = df.memory_usage().sum() / 1024**2
    if verbose: print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(end_mem, 100 * (start_mem - end_mem) / start_mem))
    return df


def add_cols(data):

    import pandas as pandas
    #Add hour, time of year, and weekend columns
    data["hour"] = data["timestamp"].dt.hour
    data["day"] = data["timestamp"].dt.day
    data["month"] = data["timestamp"].dt.month
    data["day_of_year"] = (data["timestamp"] - pandas.Timestamp("2016-01-01")).dt.days%365
    data["is_weekend"] = data["timestamp"].dt.weekday.isin([5, 6]).astype(int)

    #bucketing hours into 6 buckets: early morning, morning, afternoon, evening, late evening
    data["part_of_day"] = (data["hour"] %24 +4 ) //4


def fill_na_merged(data):
  import pandas as pandas
  import numpy as np
  print(data.head())
  data_s = data.sort_values(by=["building_id", "timestamp"])

  fill_lib = round(data_s.groupby(["building_id","day"])["cloud_coverage"].transform("mean") )
  data_s["cloud_coverage"].fillna(fill_lib, inplace=True)

  missing_cols = [col for col in data_s.columns if data_s[col].isna().any()  and col!= "cloud_coverage"]
  fill_lib = data_s.groupby(["building_id","day"])[missing_cols].transform("mean") 
  data_s.fillna(fill_lib, inplace=True)
  print(data_s.isnull().sum()/data_s.shape[0]*100)
  del fill_lib

def convert_units(data):

    data['meter_reading']=data[['site_id','meter','meter_reading']].apply(
    lambda x: x['meter_reading']*0.2931 
    if x['meter']==0 and x['site_id']==0 
    else x['meter_reading'], axis=1)


