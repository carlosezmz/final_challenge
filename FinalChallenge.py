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
    
    st_name = st_name.split(' ')
        
    if st_name[-1] in ['ST', 'RD', 'AVE', 'BL', 'DR', 'PL']:
            
        if len(st_name) == 3:
            try:
                if st_name[1][-2:] in ['TH', 'ST', 'RD', 'ND']:
                    st_name[1] = int(st_name[1][:-2])
                    st_name[1] = str(st_name[1])
                
                else:
                    st_name[1] = int(st_name[1])
                    st_name[1] = str(st_name[1])
                
            except ValueError:
                    st_name[1] = st_name[1]
                        
        elif len(st_name) == 2:
            try:
                if st_name[0][-2:] in ['TH', 'ST', 'RD', 'ND']:
                    st_name[0] = int(st_name[0][:-2])
                    st_name[0] = str(st_name[0])
                
                else:
                    st_name[0] = int(st_name[0])
                    st_name[0] = str(st_name[0])
            
            except ValueError:
                    st_name[0] = st_name[0]
        
    st_name = ' '.join(st_name)
        
    if st_name == 'BRDWAY':
        st_name = 'BROADWAY'
            
    return st_name

def check_house_number(number):
    
    if len(number) == 0:
        number = 0
    
    elif (len(number) > 0) and (type(number) != list):
        
        if '-' in number:
            number = number.split('-')
            
            if not number[1]: 
                number[1] = 0
                
            if not number[0]: 
                number[0] = 0
                
            number[0] = get_digits(number[0])
            number[1] = get_digits(number[1])
            try:
                number = (int(number[0]), int(number[1]))
                    
            except ValueError:

#                 try:
                        
                    number = (0, 0)

#                 except ValueError:
        
#                     number = (number[0], 0)
            
        else:
            number = get_digits(number)
            try:
                number = (int(number), 0)
            
            except ValueError:
                number = (number, 0)
                
                    
    elif type(number) == int:
        number = (number, 0)
                    
    elif type(number) == list:
        number[0] = get_digits(number[0])
        number = (number[0], 0)
                    
    return number

def check_county(county):
    
    boro_dict = {
        'ny':'new york', 'ne': 'new york', 'ma':'new york', 'mn':'new york', '1': 'new york',
        'bx':'bronx', 'br':'bronx', '2': 'bronx',
        'k': 'brooklyn', 'ki':'brooklyn', 'bk':'brooklyn', '3':'brooklyn',
        'q': 'queens', 'qn':'queens', 'qu':'queens', '4':'queens',
        'r': 'staten island', 's':'staten island', 'st':'staten island', 
            '5':'staten island', 'ri':'staten island'
                }
    try:
        if len(county) > 2:
            county = boro_dict[county[:2]]
            
        elif len(county) <= 2 and len(county) > 0:
            county = boro_dict[county]
        
    except ValueError:

        county = ''
        
    return county



def get_point(number, st_name, county):
    
#     import pyproj
#     from shapely.geometry import Point
    from geopy.geocoders import Nominatim

    geolocator = geopy.Nominatim(user_agent="get_latlon")
    
    if (len(number) > 0) & (len(county) > 0):
        address = '{} {}, {}, NY'.format(number, st_name, county)
                
    elif (len(number) > 0):
        address = '{} {}'.format(number, st_name)
                
    else:
        address = st_name
        
    location = geolocator.geocode(address)
    
    if location:
        return (location.longitude, location.latitude)
    
    return None

def get_zipCode(longitude, latitude):
    
    from geopy.geocoders import Nominatim
    
    geolocator = Nominatim(user_agent="get_latlon")
    
    location = geolocator.reverse((latitude, longitude))
    
    zip_code = location.raw['address']['postcode']
#     county = location.raw['address']['county']
    
    return zip_code

def get_county(longitude, latitude):
    
    from geopy.geocoders import Nominatim
    
    geolocator = Nominatim(user_agent="get_latlon")
    
    location = geolocator.reverse((latitude, longitude))
    
#     zip_code = location.raw['address']['postcode']
    county = location.raw['address']['county']
    
    return county

def rtree_idx(df):
    
#     df = df.reset_index()
    
    index = rtree.Rtree()
    
    for idx, row in df.iterrows():
        
        if not row[0]: continue
            
        
        geometry = make_polygon(row[2])
        
        if geometry:
            
            index.insert(idx, geometry.bounds)
                
    return (index, df)



def make_bounds(geom):

    lat_list = []
    lon_list = []

#     emp_list = []

    for latlon in geom.strip('MULTILINESTRING ').strip('()').split(', '):
    
        lon, lat = latlon.split(' ')
#         emp_list.append(float(lon), float(lat))
    
        lat_list.append(float(lat)) 
        lon_list.append(float(lon))
    
    if len(lon_list) > 0:
        
#         return emp_list

        return ((max(lon_list), min(lon_list)), (max(lat_list), min(lat_list)))
        
    else:
        return None
    
# def find_segment(point, index, df):
    
#     match = index.intersection((point.x, point.y, point.x, point.y))
    
#     for idx in match:
        
#         if df.geometry[idx].contains(point):
            
#             return df['PHYSICALID'][idx]
        
#     return None

def get_digits(number):
    
    import re
    
    if number:
    
        digits = re.findall(r'\d+', str(number))
    
        digits = ''.join(digits)
    
        if len(digits) > 0: return digits
    
        return 0
    return 0

def street_bounds(l_low, l_hig, r_low, r_hig):

    
    if len(l_low) == 0: l_low = (0, 0)
        
    if len(l_hig) == 0: l_hig = (0, 0)
        
    if len(r_low) == 0: r_low = (0, 0)
        
    if len(r_hig) == 0: r_hig = (0, 0)
        
    l_low = check_house_number(str(l_low))
    l_hig = check_house_number(str(l_hig))
    r_low = check_house_number(str(r_low))
    r_hig = check_house_number(str(r_hig))
        
    l_low = max(l_low, r_low)
    l_hig = max(l_hig, r_hig)
    
    return (l_low, l_hig)
        

def extract_cols(partId, records):
    
    if partId==0:
        next(records)
        
    import csv
    from datetime import datetime
    
    reader = csv.reader(records)
    
    for row in reader:
        
        if len(row) == 43:   
            
#             phy_id = int(row[0])
            county = check_county(row[21].lower())
            number = check_house_number(str(row[23]))
            year = int(datetime.strptime(row[4], '%m/%d/%Y').year)
            st_name = check_name(row[24].lower())
            
            if year not in [2015, 2016, 2017, 2018, 2019]: continue

            if county:
                
#                 if (type(number[0]) == int) & (type(number[1]) == int) & (type(number) == tuple):

                if type(number) == tuple:
            
                    yield (county, (st_name, number, year))
    
    
def extract_bounds(partID, records):
    
    import csv
    
    if partID == 0:
        next(records)
        
    reader = csv.reader(records)
    
    for row in reader:
        
        county = check_county(row[13])
        
        if len(county) > 0:
            phy_id = int(row[0])
            st_name = check_name(row[28])
            (l_low, l_hig) = street_bounds(row[1], row[3], row[4], row[5])
            
            if (l_hig != l_low) & (type(l_low) == tuple) & (type(l_hig) == tuple):
        
                yield (county, (st_name, phy_id, l_low, l_hig))
    
    
def run_spark(parking_violations, center_line):
    
    
#     parking_violations = sc.textFile(fie_dir)\
#                            .mapPartitionsWithIndex(extract_cols)
    
#     parking_violations = spark.createDataFrame(parking_violations, schema=['boro', 'st_name', 'year', 'st_numb'])
    
    
#     parking_violations = parking_violations.join(center_line, 
#                         [parking_violations.boro == center_line.boro, parking_violations.st_name == center_line.st_name], 
#                         'inner')
    
#     parking_violations = parking_violations.select('year', 'st_numb', 'phy_id', 'l_low', 'l_hig')
    
#     parking_violations = sc.textFile(fie_dir).mapPartitionsWithIndex(extract_cols)\
#                                                .reduceByKey(lambda x,y: x+y)\
#                                                .sortByKey()

    parking_violations = parking_violations.join(center_line).values()\
                        .filter(lambda x: (x[0][0] == x[1][0]) & (x[0][1] >= x[1][2]) & (x[0][1] <= x[1][3]))\
                        .map(lambda x: (x[1][1], x[0][2])).collect()

    
    return parking_violations
 
    
def conver_csv(_, records):
    
    for phy_id, count in records:
            
        yield ','.join((str(phy_id), str(count)))   
        
            
if __name__ == '__main__':
    
    sc = SparkContext()
    
    center_dir = 'hdfs:///data/share/bdm/nyc_cscl.csv'
    
    center_line = sc.textFile(center_dir)\
                .mapPartitionsWithIndex(extract_bounds)
    
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
    
#     files_list = [fie2015_dir, fie2016_dir, fie2017_dir, fie2018_dir, fie2019_dir]
    
    parking_violations = sc.textFile(fie_dir)\
                .mapPartitionsWithIndex(extract_cols)
    
    parking_violations = run_spark(parking_violations, center_line)
#     parking_violations_2016 = run_spark(sc, fie2016_dir)
#     parking_violations_2017 = run_spark(sc, fie2017_dir)
#     parking_violations_2018 = run_spark(sc, fie2018_dir)
#     parking_violations_2019 = run_spark(sc, fie2019_dir)
    
#     parking_violations = parking_violations_2015.join(parking_violations_2016)
#     parking_violations = parking_violations.join(parking_violations_2017)
#     parking_violations = parking_violations.join(parking_violations_2018)
#     parking_violations = parking_violations.join(parking_violations_2019)
    
#     parking_violations = parking_violations.mapValues(lambda x: (x[0], x[1], x[2], x[3], x[4], 
#                                                                  ((x[4]-x[3]) + (x[3]-x[2]) + (x[2]-x[1]) + (x[1]-x[0]))/4))
    
    
    
    parking_violations.mapPartitionsWithIndex(conver_csv)\
                      .saveAsTextFile('parkingViolation_count')

