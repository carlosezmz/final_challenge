"""
    @author: Carlos E. Tavarez Martinez
"""

# main
from pyspark import SparkContext
from pyspark.sql.session import SparkSession



def check_name(st_name):
    
    st_name = st_name.upper().replace('AVENUE', 'AVE')
    st_name = st_name.replace('STREET', 'ST')
    st_name = st_name.replace('ROAD', 'RD')
    st_name = st_name.replace('BOULEVARD', 'BLVD')
    st_name = st_name.replace('DRIVE', 'DR')
    st_name = st_name.replace('PLACE', 'PL')
    
#     st_name = st_name.split(' ')
        
#     if st_name[-1] in ['ST', 'RD', 'AVE', 'BL', 'DR', 'PL']:
            
#         if len(st_name) == 3:
#             try:
#                 if st_name[1][-2:] in ['TH', 'ST', 'RD', 'ND']:
#                     st_name[1] = int(st_name[1][:-2])
#                     st_name[1] = str(st_name[1])
                
#                 else:
#                     st_name[1] = int(st_name[1])
#                     st_name[1] = str(st_name[1])
                
#             except ValueError:
#                     st_name[1] = st_name[1]
                        
#         elif len(st_name) == 2:
#             try:
#                 if st_name[0][-2:] in ['TH', 'ST', 'RD', 'ND']:
#                     st_name[0] = int(st_name[0][:-2])
#                     st_name[0] = str(st_name[0])
                
#                 else:
#                     st_name[0] = int(st_name[0])
#                     st_name[0] = str(st_name[0])
            
#             except ValueError:
#                     st_name[0] = st_name[0]
        
#     st_name = ' '.join(st_name)
        
    if st_name == 'BRDWAY':
        st_name = 'BROADWAY'
            
    return st_name

def check_house_number(number):
    
    if (len(number) > 0) and (type(number) != list):
        
        if '-' in number:
            number = number.split('-')
            try:
                number = (int(number[0]), int(number[1]))
                    
            except ValueError:
                try:
                    if number[1]:
                        if number[1][0] == '0':
                            number = (int(number[0]), int(number[1][1:]))
                        
                        else:
                            number = (int(number[0][:-1]), int(number[1]))
                            
                    else:
                        number = (int(number[0]), number[1])
                except ValueError:
                    try:
                        number = (int(number[0]), int(number[1][:-1]))
                    
                    except ValueError:
                        number = number
            
        else:
            try:
                number = int(number)
            
            except ValueError:
                try:
                    number = int(number[1:])
                
                except ValueError:
                    number = number
                    
    elif type(number) == int:
        number = number
                    
    if type(number) == list:
        number = number[0]
                    
    return number

def check_county(county):
    
    boro_dict = {
        'ny':'new york', 'ne': 'new york', 'ma':'new york', 'mn':'new york', '1': 'new york',
        'bx':'bronx', 'br':'bronx', '2': 'bronx',
        'k': 'brooklyn', 'ki':'brooklyn', 'bk':'brooklyn', '3':'brooklyn',
        'q': 'queens', 'qn':'queens', 'qu':'queens', '4':'queens',
        'r': 'staten island', 's':'staten island', 'st':'staten island', '5':'staten island'
                }
    try:
        if len(county) > 2:
            county = boro_dict[county[:2]]
            
        elif len(county) <= 2 and len(county) > 0:
            county = boro_dict[county]
        
    except ValueError:

        county = boro_dict[county]
        
    return county



def get_point(number, st_name, county):
    
    import pyproj
    from shapely.geometry import Point
    from geopy.geocoders import Nominatim
    
    
    # Create an R-tree index
#     proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    
    geolocator = Nominatim(user_agent="get_latlon")
    
    point = 0
    
    if (len(number) > 0) & (len(county) > 0):
        address = '{} {}, {}, NY'.format(number, st_name, county)
                
    elif (len(number) > 0):
        address = '{} {}'.format(number, st_name)
                
    else:
        address = st_name
        
    location = geolocator.geocode(address)
    
    if location:
    
        point = Point(location.longitude, location.latitude)
    
    return point

def rtree_idx(df):
    
#     df = df.reset_index()
    
    index = rtree.Rtree()
    
    for idx, row in df.iterrows():
        
        if not row[0]: continue
            
        
        geometry = make_polygon(row[2])
        
        if geometry:
            
            index.insert(idx, geometry.bounds)
                
    return (index, df)

def make_polygon(geom):
    
    from shapely.geometry import LineString
    from shapely.geometry import Point

    emp_list = []

    for latlon in geom.strip('MULTILINESTRING ').strip('()').split(', '):
    
        lon, lat = latlon.split(' ')
    
        lat = float(lat)
        lon = float(lon)
    
        emp_list.append((lon, lat))
    
    if len(emp_list) > 1:
        emp_list = LineString(emp_list)
        return emp_list
    
    elif len(emp_list) > 0:
        emp_list = Point(emp_list)
        return emp_list
        
    else:
        return 0
    
def find_segment(point, index, df):
    
    match = index.intersection((point.x, point.y, point.x, point.y))
    
    for idx in match:
        
        if df.geometry[idx].contains(point):
            
            return df['PHYSICALID'][idx]
        
    return None
        

def extract_cols(partId, records):
    
    if partId==0:
        next(records)
        
    import csv
    import pandas as pd
    
    center_dir = 'hdfs:///data/share/bdm/nyc_cscl.csv'
    
    df_ct = pd.read_csv(center_dir)
    
    df_ct['geometry'] = df_ct['the_geom'].map(make_polygon)
#     df_ct['BOROCODE'] = df_ct['BOROCODE'].map(check_county)
    
    index, df = rtree_idx(df_ct)
    
    reader = csv.reader(records)
    
    for row in reader:
        
        if len(row) == 43:
        
            county = row[21].lower()
            number = str(row[23])
            year = int(row[4][-4:])
            st_name = check_name(row[24].lower())
            
            if year not in [2015, 2016, 2017, 2018, 2019]: 
                continue
                
#             if len(county) == 0:
#                 index, df = rtree_idx(df_ct) 
                

            county = check_county(county)
                
            point = get_point(number, st_name, county)
            
            if point == 0: continue
            
            st_int = find_segment(point, index, df)
            
            if st_int:
                yield (st_int, 1)
    
    
def run_spark(sc, fie_dir):
    
    
#     parking_violations = sc.textFile(fie_dir)\
#                            .mapPartitionsWithIndex(extract_cols)
    
#     parking_violations = spark.createDataFrame(parking_violations, schema=['boro', 'st_name', 'year', 'st_numb'])
    
    
#     parking_violations = parking_violations.join(center_line, 
#                         [parking_violations.boro == center_line.boro, parking_violations.st_name == center_line.st_name], 
#                         'inner')
    
#     parking_violations = parking_violations.select('year', 'st_numb', 'phy_id', 'l_low', 'l_hig')
    
    parking_violations = sc.textFile(fie_dir).mapPartitionsWithIndex(extract_cols)\
                                               .reduceByKey(lambda x,y: x+y)\
                                               .sortByKey()

    
    return parking_violations
 
    
def conver_csv(_, records):
    
    for phy_id, count in records:
            
        yield ','.join((str(phy_id), str(count)))   
        
            
if __name__ == '__main__':
    
    sc = SparkContext()
    
#     center_dir = 'hdfs:///data/share/bdm/nyc_cscl.csv'
    
#     center_line = spark.read.load(center_dir, format='csv', header=True, inferSchema=True)
    
#     center_line = center_line.select(
#     center_line['PHYSICALID'].alias('phy_id'),
#     center_line['L_LOW_HN'].alias('l_low'),
#     center_line['L_HIGH_HN'].alias('l_hig'),
#     center_line['BOROCODE'].cast('int').alias('boro'),
#     center_line['FULL_STREE'].alias('st_name'))
    
#     center_line = center_line.filter((center_line['l_low'].isNotNull())\
#                                      & (center_line['l_hig'].isNotNull())\
#                                      & (center_line['boro'].isNotNull())\
#                                      & (center_line['st_name'].isNotNull()))
    
    fie2015_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2015.csv'
    fie2016_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2016.csv'
    fie2017_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2017.csv'
    fie2018_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2018.csv'
    fie2019_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2019.csv'
    
    parking_violations_2015 = run_spark(sc, fie2015_dir)
    parking_violations_2016 = run_spark(sc, fie2016_dir)
    parking_violations_2017 = run_spark(sc, fie2017_dir)
    parking_violations_2018 = run_spark(sc, fie2018_dir)
    parking_violations_2019 = run_spark(sc, fie2019_dir)
    
    parking_violations = parking_violations_2015.join(parking_violations_2016)
    parking_violations = parking_violations.join(parking_violations_2017)
    parking_violations = parking_violations.join(parking_violations_2018)
    parking_violations = parking_violations.join(parking_violations_2019)
    
    parking_violations = parking_violations.mapValues(lambda x: (x[0], x[1], x[2], x[3], x[4], 
                                                                 ((x[4]-x[3]) + (x[3]-x[2]) + (x[2]-x[1]) + (x[1]-x[0]))/4))
    
    
    
    parking_violations.mapPartitionsWithIndex(conver_csv)\
                      .saveAsTextFile('parkingViolation_count')

