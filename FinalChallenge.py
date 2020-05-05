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
                    
    if type(number) == list:
        number = number[0]
                    
    return number

def check_county(county):
    
    boro_dict = {
        'ny':1, 'ne': 1, 'ma':1, 'mn':1, 'mh':1,
        'bx':2, 'br':2,
        'k': 3, 'ki':3, 'bk':3,
        'q': 4, 'qn':4, 'qu':4, 
        'r': 5, 's':5, 'st':5
                }
    
    if len(county) > 2:
        county = boro_dict[county[:2]]
            
    elif len(county) <= 2 and len(county) > 0:
        county = boro_dict[county]
        
    return county

def check_int_street(st_intc):
    
    if '/OF ' in st_intc:
        
        st_intc = st_intc.split('/OF ')[-1]
        st_intc = check_name(st_intc)
        
    return st_intc



def extract_cols(partId, records):
    
    if partId==0:
        next(records)
    
    from datetime import datetime
    import csv
    
    reader = csv.reader(records)
    for row in reader:
        
        if len(row) == 43:
        
            county = row[21]
            number = str(row[23])
            year = datetime.strptime(row[4], '%m/%d/%Y').year
            st_name = row[24].upper()
            st_intc = row[25].upper()
        
            # chechk for county
            if county:
                county = county.lower()
                county = check_county(county)
        
#                 # check for house number
#                 number = check_house_number(number)
        
                # check for street name
                st_name = check_name(st_name)
        
                # check intersection name
                st_intc = check_int_street(st_intc)
    
        
                if year in [2015, 2016, 2017, 2018, 2019]:
            
#                     if type(number) != str:
                
                    if county:
            
                        yield (county, (year, st_name, number, st_intc))
                
                
                
def extract_segment(partId, records):
    
    if partId==0:
        next(records)
    import csv
    reader = csv.reader(records)
    
    for row in reader:
        
        phy_id = int(row[0])
        boro = int(row[13])
        st_name = row[28]
        
        
        if boro and row[1]:
        
            yield (boro, (st_name, phy_id, row[1], row[3], row[4], row[5]))
            
def run_spark(sc, fie_dir, center_dir):
    
    
    parking_violations = sc.textFile(fie_dir)\
               .mapPartitionsWithIndex(extract_cols)
    
    center_line = sc.textFile(center_dir)\
                .mapPartitionsWithIndex(extract_segment)
    
    parking_violations = parking_violations.join(center_line).values()\
                                    .filter(lambda x: x[0][1] == x[1][0])\
                                    .map(lambda x: (x[0][0], x[0][1], x[0][2], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]))\
                                    .filter(lambda x: (x[2] >= x[4] and x[2] <= x[5]) or (x[2] >= x[6] and x[2] <= x[7]))\
                                    .map(lambda x: (x[3], 1))\
                                    .reduceByKey(lambda x,y: x+y)\
                                    .sortByKey()
    
    return parking_violations
            
            
            
def main(sc):
    
    center_dir = 'hdfs:///data/share/bdm/nyc_cscl.csv'
    
    fie2015_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2015.csv'
    fie2016_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2016.csv'
    fie2017_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2017.csv'
    fie2018_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2018.csv'
    fie2019_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2019.csv'
    
    parking_violations_2015 = run_spark(sc, fie2015_dir, center_dir)
    parking_violations_2016 = run_spark(sc, fie2016_dir, center_dir)
    parking_violations_2017 = run_spark(sc, fie2017_dir, center_dir)
    parking_violations_2018 = run_spark(sc, fie2018_dir, center_dir)
    parking_violations_2019 = run_spark(sc, fie2019_dir, center_dir)
    
    parking_violations = parking_violations_2015.join(parking_violations_2016)
    parking_violations = parking_violations.join(parking_violations_2017)
    parking_violations = parking_violations.join(parking_violations_2018)
    parking_violations = parking_violations.join(parking_violations_2019)
    
    parking_violations = parking_violations.mapValues(lambda x: (x[0], x[1], x[2], x[3], x[4], (x[4]-x[0])/4))
    
    parking_violations.saveAsTextFile('hdfs:////user/ctavare003/parkingViolation_count')

    
if __name__ == '__main__':
    
    sc = SparkContext()
    
    main(sc)
        