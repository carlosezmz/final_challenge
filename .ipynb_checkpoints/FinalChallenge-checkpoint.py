"""
    @author: Carlos E. Tavarez Martinez
"""

# main
from pyspark import SparkContext
# from pyspark.sql.session import SparkSession



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
        number = (0, 0)
    
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
        number = (int(number), 0)
                    
    elif type(number) == list:
        number[0] = get_digits(number[0])
        number = (number[0], 0)
                    
    return number

def check_county(county):
    
    boro_dict = {
        'ny':'new york', 'ne': 'new york', 'ma':'new york', 'mn':'new york', '1': 'new york', 'mh':'new york',
        'bx':'bronx', 'br':'bronx', '2': 'bronx',
        'k': 'brooklyn', 'ki':'brooklyn', 'bk':'brooklyn', '3':'brooklyn',
        'q': 'queens', 'qn':'queens', 'qu':'queens', '4':'queens',
        'r': 'staten island', 's':'staten island', 'st':'staten island', 
            '5':'staten island', 'ri':'staten island'
                }
    try:
        if len(county) > 2:
            county = boro_dict[county[:2]]
            
        elif (len(county) <= 2) & (len(county) > 0):
            county = boro_dict[county]
        
    except KeyError:

        county = ''
        
    return county




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

# def get_centerLine(center_dir):
    
#     from pyspark import SparkContext
#     import pandas as pd
    
#     sc = SparkContext()
    
#     center_line = sc.textFile(center_dir)\
#                     .mapPartitionsWithIndex(extract_bounds)\
#                     .collect()
    
#     center_line = pd.DataFrame(center_line, columns=['county', 
#                                                      'st_name', 
#                                                      'number', 
#                                                      'min_bound', 
#                                                      'max_ bound'])
    
#     return center_line

def get_phyID(county, st_name, number, df):
    
    
    
    phy_id = df[(df['county'] == county)\
              & (df['st_name']== st_name)\
              & (df['l_low'] <= number)\
              & (df['l_hig'] >= number)]
    

    try:
        return phy_id['phy_id'][0]
    except KeyError:
        return None
        

def extract_cols(partId, records):
    
#     center_dir = '/Users/carlostavarez/Desktop/big_data_challenge/Centerline.csv'
    
    if partId==0:
        next(records)
        
    import csv
    from datetime import datetime
    
    df = load_bounds(bounds.value)
    
    reader = csv.reader(records)
    
    for row in reader:
        
        if len(row) == 43:
            
            county = check_county(row[21].lower())
            number = check_house_number(str(row[23]))
            
            if not len(row[4]) > 9: continue
                
            year = int(datetime.strptime(row[4], '%m/%d/%Y').year)
            st_name = check_name(row[24].lower())
            
            if year != year_file.value[0]: continue

            if county in ['staten island', 'new york', 'bronx', 'brooklyn', 'queens']:
                
                if (type(number[0]) == int) & (type(number[1]) == int) & (type(number) == tuple):
                    
                    yield ((county, st_name), number, year)
                    
#                     phy_id = get_phyID(county, st_name, number, df)
                    
#                     if phy_id:
                        
#                         year_dict = {2015:0, 2016:0, 2017:0, 2018:0, 2019:0}
                        
#                         year_dict[year] = 1
                            
#                         year_t = (year_dict[2015], year_dict[2016], year_dict[2017], year_dict[2018], 
#                                               year_dict[2019])
            
#                         yield (phy_id, year_dict[2015], year_dict[2016], year_dict[2017], year_dict[2018], year_dict[2019])
    
    
    

    
# def load_bounds(bond):
    
    
#     for b in bond:
        
    
#     import pandas as pd
    
#     df = pd.DataFrame(bond, columns=['county', 'st_name', 'phy_id', 'l_low', 'l_hig'])
            
#     return df    
    
    
def extract_bounds(partID, records):
    
    if partID == 0:
        next(records)
    
    import csv
    
    reader = csv.reader(records)
    

    for row in reader:
            
        county = check_county(row[13])
        
        if county in ['staten island', 'new york', 'bronx', 'brooklyn', 'queens']:
            phy_id = int(row[0])
            st_name = check_name(row[28])
            (l_low, l_hig) = street_bounds(row[1], row[3], row[4], row[5])
            
            if (l_hig != l_low) & (type(l_low) == tuple) & (type(l_hig) == tuple):
        
                    yield ((county, st_name), phy_id, l_low, l_hig)

            
            

def rdd_union(sc, files_list):
    
    for idx, file in enumerate(files_list):
        
        if idx == 0:
            
            rdd = sc.textFile(file).mapPartitionsWithIndex(extract_cols).cache()
            
            rdds = rdd
            
        else:
            
            rdd = sc.textFile(file).mapPartitionsWithIndex(extract_cols).cache()
            
            rdds = rdds.union(rdd).cache()
            
#     rdds = rdds.distinct()
            
#     rdds = rdds.map(lambda x: (x[0], x[2]))

            
    return rdds.cache()

def get_id(partID, records):
    
    for row in records:
        
        if row[0][0] != row[1][0]: continue
            
        if (row[0][0] >= row[1][1]) & (row[0][0] <= row[1][2]):
            
            year_d = {2015:0, 2016:0, 2017:0, 2018:0, 2019:0}
            
            if row[0][2] in year_d:
                
                year_d[row[0][2]] = 1
                
                year_t = (year_d[2015], year_d[2016], year_d[2017], year_d[2018], year_d[2019])
            
                yield (row[0][1], year_t)
    
 
    
def reduce_csv(_, records):
    
    old_phy_id = None
    current_phy_id = None
    x = 0
    old_x = 0
    
    for values in records:
        
        phy_id = values[0]
        
        if (phy_id != current_phy_id) & (current_phy_id == None):
            
            current_phy_id = phy_id
            x = values[1:]
            
        elif (phy_id == current_phy_id):
            
            x[0] += values[1]
            x[1] += values[2]
            x[2] += values[3]
            x[3] += values[4]
            x[4] += values[5]
            
        else:
            
            old_phy_id = current_phy_id
            current_phy_id = phy_id
            
            old_x = x
            x = values[1:]
            
            rate = ((old_x[4]-old_x[3])+(old_x[3]-old_x[2])+(old_x[2]-old_x[1])+(old_x[1]-old_x[0]))/4
            
            yield (old_phy_id, old_x[0], old_x[1], old_x[2], old_x[3], old_x[4], rate)
        
            
if __name__ == '__main__':
    
    sc = SparkContext()
    

    center_dir = '/data/share/bdm/nyc_cscl.csv'
    fie2015_dir = '/data/share/bdm/nyc_parking_violation/2015.csv'
    fie2016_dir = '/data/share/bdm/nyc_parking_violation/2016.csv'
    fie2017_dir = '/data/share/bdm/nyc_parking_violation/2017.csv'
    fie2018_dir = '/data/share/bdm/nyc_parking_violation/2018.csv'
    fie2019_dir = '/data/share/bdm/nyc_parking_violation/2019.csv'
    
    files_list = [fie2015_dir, fie2016_dir, fie2017_dir, fie2018_dir, fie2019_dir]
    
#     parking_violations = rdd_union(sc, files_list).collect()
    
#     bounds = sc.textFile(center_dir).mapPartitionsWithIndex(extract_bounds)
    
#     bounds = sc.broadcast(bounds)
    
    
#     parking_violations = rdd_union(sc, files_list)

    
    for idx, file in enumerate(files_list):
        
        year = int(file[-8:-4])
        
        year_file = sc.broadcast([year])
        
            
        rdd = sc.textFile(file)\
                .mapPartitionsWithIndex(extract_cols).collect()
#                 .filter(lambda x: x[1][3] == year).join(bounds).values()\
#                 .mapPartitionsWithIndex(get_id)
            
#         rdd = rdd.join(bounds)\
#                  .values()\
#                  .mapPartitionsWithIndex(get_id)\
#                      .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4])).cache()
#                      .sortByKey().cache()
            
#         if idx == 0:
            
        parking_violations_list += rdd
            
#         else:
            
#             parking_violations_list = parking_violations_list.union(rdd).cache()
            
#         else:
            
#             rdd = sc.textFile(file)\
#                     .mapPartitionsWithIndex(extract_cols)\
#                     .filter(lambda x: x[1][3] == year).cache()
            
#             rdd = rdd.join(bounds)\
#                      .values()\
#                      .mapPartitionsWithIndex(get_id).sortByKey().collect()
#                      .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4]))\
#                      .sortByKey().cache()
            
            
#             parking_violations = parking_violations.union(rdd).cache()
            
    
    parking_violations = sc.parallelize(parking_violations_list)

#     year_file = sc.broadcast([2015])

#     parking_violations = sc.textFile(fie2015_dir)\
#                            .mapPartitionsWithIndex(extract_cols)\
    
    parking_violations = parking_violations.join(bounds).values()\
                                           .filter(lambda x: (x[0][0] >= x[1][1]) & (x[0][0] <= x[1][2]))\
                                           .sortByKey()\
                                           .mapPartitionsWithIndex(reduce_csv)\
#                                            .saveAsTextFile('parkingCount')


    

    
#     parking_violations.mapPartitionsWithIndex(reduce_csv).saveAsTextFile('Violations')
    parking_violations.saveAsTextFile('parkingCount')

