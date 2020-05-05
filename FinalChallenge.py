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
    import csv
    reader = csv.reader(records)
    for row in reader:
        
        if len(row) == 43:
        
            county = row[21]
            number = str(row[23])
            year = int(row[4][-4:])
            st_name = row[24].upper()
            st_intc = row[25].upper()
        
            # chechk for county
            if county:
                county = county.lower()
                county = check_county(county)
        
#                 # check for house number
                number = check_house_number(number)
        
                # check for street name
                st_name = check_name(st_name)
        
                # check intersection name
                st_intc = check_int_street(st_intc)
    
        
                if year in [2015, 2016, 2017, 2018, 2019]:
            
                    if type(number) != str:
                
                        if county:
            
                            yield (county, st_name, year, number)

                
def get_phyID(partID, records):

    for row in records:
        
        year = row['year']
        st_numb = row['st_numb']
        phy_id = row['phy_id']
        l_low = check_house_number(row['l_low'])
        l_hig = check_house_number(row['l_hig'])
        
        if (type(st_numb) == type(l_low)) & (type(st_numb) == type(l_hig)):
            
            if (st_numb >= l_low) & (st_numb <= l_hig):
                
                yield (phy_id, 1)
    
    
def run_spark(sc, spark, fie_dir, center_line):
    
    
    parking_violations = sc.textFile(fie_dir)\
                           .mapPartitionsWithIndex(extract_cols)
    
    parking_violations = spark.createDataFrame(parking_violations, schema=['boro', 'st_name', 'year', 'st_numb'])
    
    
    parking_violations = parking_violations.join(center_line, 
                        [parking_violations.boro == center_line.boro, parking_violations.st_name == center_line.st_name], 
                        'inner')
    
    parking_violations = parking_violations.select('year', 'st_numb', 'phy_id', 'l_low', 'l_hig')
    
    parking_violations = parking_violations.rdd.mapPartitionsWithIndex(get_phyID)\
                                               .reduceByKey(lambda x,y: x+y)\
                                               .sortByKey()

    
    return parking_violations
            
            
            
def main(sc, spark):
    
    center_dir = 'hdfs:///data/share/bdm/nyc_cscl.csv'
    
    center_line = spark.read.load(center_dir, format='csv', header=True, inferSchema=True)
    
    center_line = center_line.select(
    center_line['PHYSICALID'].cast('int').alias('phy_id'),
    center_line['L_LOW_HN'].alias('l_low'),
    center_line['L_HIGH_HN'].alias('l_hig'),
    center_line['BOROCODE'].cast('int').alias('boro'),
    center_line['FULL_STREE'].alias('st_name'))
    
    center_line = center_line.filter((center_line['l_low'].isNotNull())\
                                     & (center_line['l_hig'].isNotNull())\
                                     & (center_line['boro'].isNotNull())\
                                     & (center_line['st_name'].isNotNull()))
    
    fie2015_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2015.csv'
    fie2016_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2016.csv'
    fie2017_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2017.csv'
    fie2018_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2018.csv'
    fie2019_dir = 'hdfs:///data/share/bdm/nyc_parking_violation/2019.csv'
    
    parking_violations_2015 = run_spark(sc, spark, fie2015_dir, center_line)
    parking_violations_2016 = run_spark(sc, spark, fie2016_dir, center_line)
    parking_violations_2017 = run_spark(sc, spark, fie2017_dir, center_line)
    parking_violations_2018 = run_spark(sc, spark, fie2018_dir, center_line)
    parking_violations_2019 = run_spark(sc, spark, fie2019_dir, center_line)
    
    parking_violations = parking_violations_2015.join(parking_violations_2016)
    parking_violations = parking_violations.join(parking_violations_2017)
    parking_violations = parking_violations.join(parking_violations_2018)
    parking_violations = parking_violations.join(parking_violations_2019)
    
    parking_violations = parking_violations.mapValues(lambda x: (x[0], x[1], x[2], x[3], x[4], 
                                                                 ((x[4]-x[3]) + (x[3]-x[2]) + (x[2]-x[1]) + (x[1]-x[0]))/4))
    
    parking_violations.saveAsTextFile('hdfs:////user/ctavare003/parkingViolation_count')

    
if __name__ == '__main__':
    
    sc = SparkContext()
    spark = SparkSession(sc)
    
    main(sc, spark)
        